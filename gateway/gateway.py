"""网关编排：路由 → 去重(跨灰度版本, 领导者查缓存) → 背压 → 合批 → 重试/降级 → 统计。

统计守恒：每次 infer 调用 accepted +1，最终恰好落入
succeeded / failed / rejected 之一；重试与降级不重复计 real_inferences。
分版本统计与整体口径一致：各版本 succeeded/failed/rejected/real_inferences/
fallbacks 之和等于整体对应计数。
"""
from __future__ import annotations

import asyncio

from gateway.admission import Admission
from gateway.backends import BackendError, ItemFailure, LocalBackend
from gateway.batching import BatchScheduler
from gateway.cache import ResultCache
from gateway.config import BreakerConfig, GatewayConfig, ModelSpec, VersionSpec
from gateway.dedup import FOLLOWER, LEADER, DedupRegistry, UsageStats
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

        async def run_work() -> tuple[str, str, bool]:
            # 领导者先查本版本缓存：不同 request_id 但同规范化输入仍可命中缓存；
            # 跟随者直接共享领导者结果，不再各自查缓存。
            cached = self._cache.get(model, vspec.version, prompt)
            if cached is not None:
                self._stats.cache_hits += 1
                return cached, vspec.version, True
            self._stats.real_inferences += 1  # 每个逻辑请求只计一次，重试不重复计
            executed = [vspec.version]  # 实际产生结果的版本（降级时更新）
            performed = False
            try:
                result = await self._execute(model_spec, vspec, prompt, executed)
                performed = True
                return result, executed[0], False
            except Exception as exc:
                performed = True
                # 让去重层知道终态归属版本（回退封闭判定需要）
                exc._dedup_version = executed[0]  # type: ignore[attr-defined]
                raise
            finally:
                # 只有真实推理才计入分版本用量；缓存命中不计
                if performed:
                    self._version_stats.get(model, executed[0]).real_inferences += 1

        # 带 request_id 的提交：去重优先——重复提交即使被灰度路由到不同版本，
        # 也必须共享同一次真实推理与同一个结果（缓存检查只由领导者做一次）。
        if request_id is not None:
            result, produced, role = await self._dedup.run_once(
                model, vspec.version, request_id, run_work
            )
            if role is FOLLOWER:
                # 跟随者：复用在途/已完成结果（领导者发布时已写缓存）
                self._stats.dedup_hits += 1
            elif role is LEADER:
                # 领导者且结果未被回退封闭：按实际产生结果的版本写入缓存
                self._cache.put(model, produced, prompt, result)
            # SEALED：领导者执行在回退期间完成，结果只返回给本提交，
            # 不发布为去重结果、不写缓存。
            return result

        # 无 request_id：直接执行（内部先查本版本缓存）
        result, produced, from_cache = await run_work()
        if not from_cache:
            self._cache.put(model, produced, prompt, result)
        return result

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
                return await self._infer_with_retry(model_spec, vspec, prompt)
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
                return result
            if model_spec.fallback:
                raise GatewayError(
                    Reason.FALLBACK_UNAVAILABLE, f"all fallbacks unavailable for {model!r}"
                )
            raise GatewayError(Reason.BACKEND_ERROR, f"backend error on {model}@{vspec.version}")
        finally:
            await self._admission.release(model)

    async def _infer_with_retry(
        self, model_spec: ModelSpec, vspec: VersionSpec, prompt: str
    ) -> str:
        model = model_spec.name
        backend = self._backends[(model, vspec.version)]
        last: BackendError | None = None
        for _ in range(self._config.retry.max_attempts):
            try:
                return await self._batch.submit(
                    (model, vspec.version), prompt, backend.infer_batch
                )
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
        """一键回退：仅收敛目标别名；清除被撤销版本此前产生的缓存与去重结果。

        - 作用域只限目标别名：其它别名与降级链不受影响，继续承接同一版本；
        - 被撤销版本不做全局下线：显式访问或经其它别名访问照常服务；
        - 被撤销版本此前产生的缓存结果与重复提交记录不再被返回（版本代际 +1，
          连在途共享的去重记录也一并封闭，只交给已在等待的提交正常收尾）；
        - 在途请求已完成路由解析，不受回退影响，正常结束、计数不乱；
        - 重试、降级、缓存容量上限与排队行为保持不变。
        """
        revoked = self._registry.rollback(model, alias, version)
        for v in revoked:
            self._cache.purge_version(model, v)
            await self._dedup.revoke_version((model, v))
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
