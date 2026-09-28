"""网关编排：路由 → 去重(含缓存) → 背压 → 合批 → 重试/降级 → 统计。

统计守恒：每次 infer 调用 accepted +1，最终恰好落入
succeeded / failed / rejected 之一；重试与降级不重复计 real_inferences。
分版本统计与整体口径一致：各版本 succeeded/failed/rejected/real_inferences/
fallbacks 之和等于整体对应计数。

去重作用域为 (model, request_id)：同一 request_id 即使在灰度加权分流中
被解析到不同版本，也只有第一次提交真实推理，所有提交共享一致结果。
"""
from __future__ import annotations

import asyncio

from gateway.admission import Admission
from gateway.backends import BackendError, ItemFailure, LocalBackend
from gateway.batching import BatchScheduler
from gateway.cache import ResultCache
from gateway.config import BreakerConfig, GatewayConfig, ModelSpec, VersionSpec
from gateway.dedup import DedupRegistry, UsageStats
from gateway.errors import GatewayError, Reason
from gateway.registry import Registry
from gateway.rollout import BreakerRegistry, VersionStatsRegistry


class Gateway:
    def __init__(self, config: GatewayConfig) -> None:
        self._config = config
        self._registry = Registry(config)
        self._cache = ResultCache(config.cache)
        self._admission = Admission(config.admission)
        self._batch = BatchScheduler(config.batch)
        self._dedup = DedupRegistry()
        self._stats = UsageStats()
        self._version_stats = VersionStatsRegistry()
        self._breakers = BreakerRegistry(self._on_breaker_trip)
        for name, spec in config.models.items():
            for version, bcfg in spec.breakers.items():
                self._breakers.configure(name, version, bcfg)
        self._backends: dict[tuple[str, str], LocalBackend] = {
            (name, v): LocalBackend(name, vspec)
            for name, spec in config.models.items()
            for v, vspec in spec.versions.items()
        }
        self._ready = False
        self._closing = False
        self._inflight = 0
        self._inflight_cond: asyncio.Condition | None = None

    # ---- 生命周期 ----

    async def start(self) -> None:
        """预热：对每个未下线版本后端做一次真实探测，全部可用才就绪。"""
        for (model, version), backend in self._backends.items():
            if self._registry.is_retired(model, version):
                continue
            await backend.infer_batch(["__warmup__"])
        self._inflight_cond = asyncio.Condition()
        self._ready = True

    async def shutdown(self) -> None:
        """优雅停机：拒绝新请求，等待在途请求完成后冲刷合批器。"""
        self._closing = True
        self._ready = False
        assert self._inflight_cond is not None
        async with self._inflight_cond:
            while self._inflight > 0:
                await self._inflight_cond.wait()
        await self._batch.shutdown()

    @property
    def ready(self) -> bool:
        return self._ready and not self._closing

    # ---- 主入口 ----

    async def infer(
        self,
        model: str,
        prompt: str,
        version_or_alias: str | None = None,
        request_id: str | None = None,
    ) -> str:
        stats = self._stats
        stats.accepted += 1
        if self._closing:
            stats.rejected += 1
            raise GatewayError(Reason.SHUTTING_DOWN, "gateway is shutting down")
        if not self._ready:
            stats.rejected += 1
            raise GatewayError(Reason.NOT_READY, "gateway is not ready")
        assert self._inflight_cond is not None
        async with self._inflight_cond:
            self._inflight += 1
        try:
            result = await self._infer_inner(model, prompt, version_or_alias, request_id)
            stats.succeeded += 1
            return result
        except GatewayError as exc:
            # failed 仅计后端/降级失败；路由、背压、生命周期问题计 rejected
            if exc.reason in (Reason.BACKEND_ERROR, Reason.FALLBACK_UNAVAILABLE):
                stats.failed += 1
            else:
                stats.rejected += 1
            raise
        finally:
            async with self._inflight_cond:
                self._inflight -= 1
                self._inflight_cond.notify_all()

    async def _infer_inner(
        self,
        model: str,
        prompt: str,
        version_or_alias: str | None,
        request_id: str | None,
    ) -> str:
        # 1. 路由（单请求只解析一次，绝不跨版本混合；加权别名在此选定唯一版本）
        model_spec, vspec = self._registry.resolve(model, version_or_alias)
        vstats = self._version_stats.get(model, vspec.version)
        vstats.requests += 1
        try:
            result = await self._serve(model_spec, vspec, prompt, request_id)
        except GatewayError as exc:
            if exc.reason in (Reason.BACKEND_ERROR, Reason.FALLBACK_UNAVAILABLE):
                vstats.failed += 1
                self._breakers.record(model, vspec.version, failed=True)
            else:
                vstats.rejected += 1  # 背压等解析后拒绝，不计入止损窗口
            raise
        vstats.succeeded += 1
        self._breakers.record(model, vspec.version, failed=False)
        return result

    async def _serve(
        self,
        model_spec: ModelSpec,
        vspec: VersionSpec,
        prompt: str,
        request_id: str | None,
    ) -> str:
        model = model_spec.name

        async def work() -> tuple[str, str]:
            """领导者真实执行；返回 (结果, 实际产生结果的版本)。"""
            # 缓存（键含解析后的具体版本）只在领导者路径查询：同一
            # request_id 的跟随者直接共享领导者结果，避免灰度分流到不同
            # 版本时先后命中不同版本缓存而返回不一致结果。
            cached = self._cache.get(model, vspec.version, prompt)
            if cached is not None:
                self._stats.cache_hits += 1
                return cached, vspec.version
            self._stats.real_inferences += 1  # 每个逻辑请求只计一次，重试不重复计
            executed = [vspec.version]  # 实际产生结果的版本（降级时更新）
            try:
                result = await self._execute(model_spec, vspec, prompt, executed)
                return result, executed[0]
            finally:
                self._version_stats.get(model, executed[0]).real_inferences += 1

        # 去重作用域为 (model, request_id)：灰度下同一 request_id 即使被
        # 解析到不同版本，也只有第一次提交真实推理，结果全局一致。
        if request_id is not None:
            result, is_leader = await self._dedup.run_once(
                model, request_id, vspec.version, work
            )
            if not is_leader:
                self._stats.dedup_hits += 1
            return result

        # 无 request_id：保持缓存先行
        cached = self._cache.get(model, vspec.version, prompt)
        if cached is not None:
            self._stats.cache_hits += 1
            return cached
        return (await work())[0]

    async def _execute(
        self,
        model_spec: ModelSpec,
        vspec: VersionSpec,
        prompt: str,
        executed: list[str],
    ) -> str:
        model = model_spec.name
        # 4. 背压：获取执行许可（超限/超时在此拒绝）
        await self._admission.acquire(model)
        try:
            # 5. 合批 + 重试
            try:
                return await self._infer_with_retry(model, vspec, prompt)
            except ItemFailure as exc:
                # 单项确定性失败：不重试、不降级
                raise GatewayError(Reason.BACKEND_ERROR, str(exc)) from exc
            except BackendError:
                pass
            # 6. 降级链：目标可用性真实反映，不可用则明确失败
            for fb_version in model_spec.fallback:
                if self._registry.is_retired(model, fb_version):
                    continue  # 已下线的降级目标真实跳过
                fb_spec = model_spec.versions.get(fb_version)
                if fb_spec is None:
                    continue
                try:
                    result = await self._batch.submit(
                        (model, fb_version), prompt, self._backends[(model, fb_version)].infer_batch
                    )
                except BackendError:
                    continue
                self._stats.fallbacks += 1
                self._version_stats.get(model, vspec.version).fallbacks += 1
                executed[0] = fb_version  # 实际推理由降级目标版本执行
                # 缓存键使用实际产生结果的版本
                self._cache.put(model, fb_version, prompt, result)
                return result
            if model_spec.fallback:
                raise GatewayError(
                    Reason.FALLBACK_UNAVAILABLE, f"all fallbacks unavailable for {model!r}"
                )
            raise GatewayError(Reason.BACKEND_ERROR, f"backend error on {model}@{vspec.version}")
        finally:
            await self._admission.release(model)

    async def _infer_with_retry(self, model: str, vspec: VersionSpec, prompt: str) -> str:
        backend = self._backends[(model, vspec.version)]
        last: BackendError | None = None
        for _ in range(self._config.retry.max_attempts):
            try:
                result = await self._batch.submit(
                    (model, vspec.version), prompt, backend.infer_batch
                )
                self._cache.put(model, vspec.version, prompt, result)
                return result
            except ItemFailure:
                raise
            except BackendError as exc:
                last = exc
        assert last is not None
        raise last

    # ---- 灰度 / 止损 / 回退 ----

    def _on_breaker_trip(self, model: str, version: str) -> None:
        """自动止损触发：把该版本从所有别名分流中摘除，其余版本继续服务。"""
        self._registry.trip_version(model, version)

    def set_breaker(self, model: str, version: str, config: BreakerConfig) -> None:
        """运行期为某版本配置/更新自动止损参数。"""
        if (model, version) not in self._backends:
            raise GatewayError(Reason.UNKNOWN_VERSION, f"unknown version: {model}@{version}")
        self._breakers.configure(model, version, config)

    def reset_breaker(self, model: str, version: str) -> None:
        """解除某版本的止损状态（需另行恢复权重才会重新放量）。"""
        self._breakers.reset(model, version)
        self._registry.reset_trip(model, version)

    async def rollback(self, model: str, alias: str, version: str) -> list[str]:
        """一键回退：仅收敛目标别名；清除被撤销版本的缓存与去重记录。

        - 作用域仅限该别名：其它别名、显式版本访问、重试与降级链均不受影响；
        - 被撤销版本此前产生的缓存结果与 request_id 去重记录被清除，
          其在途去重条目的发布资格一并摘除（在途请求仍照常收尾，但旧
          版本结果不会在回退后成为可复用记录）；
        - 保留版本的缓存/去重记录原样保留。
        """
        revoked = self._registry.rollback(model, alias, version)
        if revoked:
            for v in revoked:
                self._cache.purge_version(model, v)
            await self._dedup.purge_versions(model, revoked)
        return revoked

    def version_stats_snapshot(self) -> dict[str, dict[str, int]]:
        return self._version_stats.snapshot()

    # ---- 测试/管理接口 ----

    @property
    def registry(self) -> Registry:
        return self._registry

    @property
    def backends(self) -> dict[tuple[str, str], LocalBackend]:
        return self._backends

    @property
    def stats(self) -> UsageStats:
        return self._stats
