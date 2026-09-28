"""灰度放量、自动止损与一键回退。

- 加权别名分流：分布接近配置权重；权重热更新即时生效；权重 0 不再接新请求；
- 自动止损：失败比例达阈值后版本停止放量，其余版本继续服务，在途请求正常收尾；
- 一键回退：别名收敛单版本，被撤销版本的缓存/去重结果不再返回，在途请求不受影响；
- 分版本统计与整体统计守恒（并发、重复提交、降级、背压场景）。
"""
from __future__ import annotations

import asyncio

import httpx
import pytest

from gateway.app import create_app
from gateway.config import (
    AdmissionConfig,
    BatchConfig,
    BreakerConfig,
    CacheConfig,
    GatewayConfig,
    ModelSpec,
    RetryConfig,
    VersionSpec,
)
from gateway.errors import GatewayError, Reason
from gateway.gateway import Gateway
from tests.conftest import log


def rollout_config(
    *,
    weights: dict[str, dict[str, float]] | None = None,
    breakers: dict[str, BreakerConfig] | None = None,
    fail_prompts_v2: frozenset[str] = frozenset(),
    v2_latency: float = 5.0,
    admission: AdmissionConfig | None = None,
    cache: CacheConfig | None = None,
) -> GatewayConfig:
    return GatewayConfig(
        models={
            "m": ModelSpec(
                name="m",
                versions={
                    "v1": VersionSpec(version="v1", latency_ms=5.0),
                    "v2": VersionSpec(
                        version="v2", latency_ms=v2_latency, fail_prompts=fail_prompts_v2
                    ),
                },
                aliases={"latest": "v1"},
                alias_weights=weights or {},
                fallback=("v2",),
                breakers=breakers or {},
            ),
        },
        batch=BatchConfig(window_ms=5.0, max_batch_size=8, max_wait_ms=100.0),
        admission=admission or AdmissionConfig(),
        cache=cache or CacheConfig(capacity=1024, enabled=True),
        retry=RetryConfig(max_attempts=2),
    )


async def test_weighted_split_distribution():
    """多版本分流下，足够多的独立请求体现接近配置权重的分布。"""
    gw = Gateway(
        rollout_config(
            weights={"latest": {"v1": 1.0, "v2": 3.0}},
            admission=AdmissionConfig(
                global_concurrency=64,
                global_queue=512,
                per_model_concurrency=64,
                per_model_queue=512,
            ),
        )
    )
    await gw.start()
    try:
        results = await asyncio.gather(
            *(gw.infer("m", f"dist-{i}", request_id=f"dist-{i}") for i in range(400))
        )
        v1 = sum(1 for r in results if r.startswith("m@v1#"))
        v2 = sum(1 for r in results if r.startswith("m@v2#"))
        log.info("dist: v1=%d v2=%d frac_v2=%.3f", v1, v2, v2 / len(results))
        assert v1 + v2 == len(results)  # 每个结果只归属一个版本
        assert 0.65 <= v2 / len(results) <= 0.85
    finally:
        await gw.shutdown()


async def test_weight_hot_update_under_concurrency():
    """并发下热更新权重：单请求不跨版本；权重 0 的版本不再接新请求。"""
    gw = Gateway(rollout_config(weights={"latest": {"v1": 1.0}}))
    await gw.start()
    try:
        stop = asyncio.Event()
        results: list[str] = []

        async def worker(wid: int) -> None:
            i = 0
            while not stop.is_set():
                results.append(
                    await gw.infer("m", f"hot-{wid}-{i}", request_id=f"hot-{wid}-{i}")
                )
                i += 1

        workers = [asyncio.create_task(worker(w)) for w in range(4)]
        await asyncio.sleep(0.1)
        gw.registry.set_alias_weights("m", "latest", {"v1": 1.0, "v2": 1.0})
        await asyncio.sleep(0.2)
        gw.registry.set_alias_weights("m", "latest", {"v1": 0.0, "v2": 1.0})  # v1 权重归零
        await asyncio.sleep(0.2)
        stop.set()
        await asyncio.gather(*workers)
        log.info("hot-update: %d results, all single-version", len(results))
        assert results
        assert all(r.startswith("m@v1#") or r.startswith("m@v2#") for r in results)
        final = await asyncio.gather(
            *(gw.infer("m", f"final-{i}", request_id=f"final-{i}") for i in range(20))
        )
        assert all(r.startswith("m@v2#") for r in final)  # 权重 0 后 v1 不再接新请求
    finally:
        await gw.shutdown()


async def test_breaker_trips_and_inflight_requests_finish():
    """失败比例达阈值自动止损：该版本停止放量，其余版本继续，在途请求正常收尾。"""
    gw = Gateway(
        rollout_config(
            weights={"latest": {"v1": 1.0, "v2": 1.0}},
            breakers={"v2": BreakerConfig(failure_ratio=0.5, window=4)},
            fail_prompts_v2=frozenset({"bad"}),
            v2_latency=120.0,
        )
    )
    await gw.start()
    try:
        # 在途慢请求（显式 v2，正常输入）
        inflight = [
            asyncio.create_task(
                gw.infer("m", f"slow-{i}", version_or_alias="v2", request_id=f"slow-{i}")
            )
            for i in range(3)
        ]
        # 并发打入失败请求触发 v2 止损
        outcomes = await asyncio.gather(
            *(gw.infer("m", "bad", request_id=f"bad-{i}") for i in range(30)),
            return_exceptions=True,
        )
        failures = [o for o in outcomes if isinstance(o, GatewayError)]
        log.info("breaker: %d failures among 30, tripped=%s", len(failures),
                 gw.registry.is_tripped("m", "v2"))
        assert gw.registry.is_tripped("m", "v2")
        # 在途请求正常结束，结果来自 v2，无半更新
        slow_results = await asyncio.gather(*inflight)
        assert all(r.startswith("m@v2#") for r in slow_results)
        # 止损后新请求全部由 v1 承接（"bad" 在 v1 上可成功）
        r = await gw.infer("m", "bad", request_id="bad-after-trip")
        assert r.startswith("m@v1#")
        vs = gw.version_stats_snapshot()
        log.info("breaker version stats=%s", vs)
        assert vs["m@v2"]["failed"] >= 4
        s = gw.stats.snapshot()
        assert s["accepted"] == s["succeeded"] + s["failed"] + s["rejected"]
    finally:
        await gw.shutdown()


async def test_rollback_revokes_cache_and_dedup():
    """一键回退：别名收敛单版本；被撤销版本的缓存与去重结果不再被返回。"""
    gw = Gateway(rollout_config(weights={"latest": {"v1": 1.0, "v2": 1.0}}))
    await gw.start()
    try:
        r_v2 = await gw.infer("m", "keep-v2", version_or_alias="v2", request_id="rb-v2")
        r_v1 = await gw.infer("m", "keep-v1", version_or_alias="v1", request_id="rb-v1")
        assert r_v2.startswith("m@v2#") and r_v1.startswith("m@v1#")
        cache_before = len(gw._cache)

        revoked = await gw.rollback("m", "latest", "v1")
        log.info("rollback: revoked=%s cache %d -> %d", revoked, cache_before, len(gw._cache))
        assert revoked == ["v2"]
        assert len(gw._cache) == cache_before - 1  # v2 缓存被清除，v1 保留

        # 新请求经别名全部到 v1
        rs = await asyncio.gather(
            *(gw.infer("m", f"post-{i}", request_id=f"post-{i}") for i in range(10))
        )
        assert all(r.startswith("m@v1#") for r in rs)
        # 显式访问被撤销版本 -> 仍然可用：回退不下线版本，只收敛目标别名
        r_exp = await gw.infer("m", "keep-v2", version_or_alias="v2", request_id="rb-v2-exp")
        assert r_exp == r_v2 and r_exp.startswith("m@v2#")
        # 旧 request_id 的去重记录不再返回 v2 结果：经别名重新在 v1 执行
        r2 = await gw.infer("m", "keep-v2", request_id="rb-v2")
        assert r2.startswith("m@v1#")
        # v1 旧缓存仍可命中（回退不破坏既有缓存行为）
        r1c = await gw.infer("m", "keep-v1", version_or_alias="v1", request_id="rb-v1-again")
        assert r1c == r_v1
        assert gw.stats.snapshot()["cache_hits"] >= 1
    finally:
        await gw.shutdown()


async def test_rollback_does_not_disturb_inflight():
    """回退不影响正在处理中的请求。"""
    gw = Gateway(
        rollout_config(weights={"latest": {"v1": 1.0, "v2": 1.0}}, v2_latency=150.0)
    )
    await gw.start()
    try:
        slow = asyncio.create_task(
            gw.infer("m", "slow-rb", version_or_alias="v2", request_id="slow-rb")
        )
        await asyncio.sleep(0.05)
        revoked = await gw.rollback("m", "latest", "v1")
        assert revoked == ["v2"]
        r = await slow  # 在途请求正常完成，结果仍来自 v2
        log.info("rollback inflight result=%s", r)
        assert r.startswith("m@v2#")
        s = gw.stats.snapshot()
        assert s["accepted"] == s["succeeded"] + s["failed"] + s["rejected"]
    finally:
        await gw.shutdown()


async def test_version_stats_conservation_mixed():
    """并发 + 重复提交 + 降级 + 缓存命中下，分版本统计与整体统计守恒。"""
    gw = Gateway(rollout_config(weights={"latest": {"v1": 1.0, "v2": 1.0}}))
    await gw.start()
    try:
        # 缓存命中：显式 v2 同 prompt 两次
        await gw.infer("m", "cache-p", version_or_alias="v2", request_id="c-1")
        await gw.infer("m", "cache-p", version_or_alias="v2", request_id="c-2")
        # 降级：v1 后端持续故障，v1 归属请求降级到 v2
        gw.backends[("m", "v1")].transient_failures = 10_000
        # 并发别名请求 + 重复提交
        await asyncio.gather(
            *(gw.infer("m", f"mix-{i}", request_id=f"mix-{i}") for i in range(20)),
            *(gw.infer("m", "dup-p", request_id="dup-x") for _ in range(4)),
        )
        s = gw.stats.snapshot()
        vs = gw.version_stats_snapshot()
        log.info("conservation: overall=%s versions=%s", s, vs)
        assert s["accepted"] == s["succeeded"] + s["failed"] + s["rejected"]
        for key in ("succeeded", "failed", "rejected", "real_inferences", "fallbacks"):
            assert sum(v[key] for v in vs.values()) == s[key], key
        assert sum(v["requests"] for v in vs.values()) == s["accepted"]
        for name, v in vs.items():
            assert v["requests"] == v["succeeded"] + v["failed"] + v["rejected"], name
        assert vs["m@v1"]["fallbacks"] == s["fallbacks"] > 0  # 降级都归属 v1
        assert vs["m@v2"]["real_inferences"] == s["real_inferences"]  # 实际执行都在 v2
    finally:
        await gw.shutdown()


async def test_version_stats_conservation_with_rejections():
    """背压拒绝（路由解析后）也计入分版本口径并保持守恒。"""
    gw = Gateway(
        rollout_config(
            weights={"latest": {"v1": 1.0, "v2": 1.0}},
            v2_latency=150.0,
            admission=AdmissionConfig(
                global_concurrency=2,
                global_queue=0,
                per_model_concurrency=2,
                per_model_queue=0,
                wait_timeout_ms=50.0,
            ),
        )
    )
    await gw.start()
    try:
        outcomes = await asyncio.gather(
            *(gw.infer("m", f"bp-{i}", request_id=f"bp-{i}") for i in range(10)),
            return_exceptions=True,
        )
        rejected = sum(
            1 for o in outcomes if isinstance(o, GatewayError) and o.reason is Reason.QUEUE_FULL
        )
        log.info("backpressure: rejected=%d", rejected)
        assert rejected > 0
        s = gw.stats.snapshot()
        vs = gw.version_stats_snapshot()
        assert s["rejected"] == rejected
        assert sum(v["rejected"] for v in vs.values()) == s["rejected"]
        assert sum(v["requests"] for v in vs.values()) == s["accepted"]
        for v in vs.values():
            assert v["requests"] == v["succeeded"] + v["failed"] + v["rejected"]
    finally:
        await gw.shutdown()


async def test_http_weights_rollback_breaker_and_version_stats():
    """HTTP：权重热更新、一键回退、止损配置与分版本统计入口。"""
    app = create_app(
        rollout_config(
            weights={"latest": {"v1": 1.0, "v2": 1.0}},
            breakers={"v2": BreakerConfig(failure_ratio=0.5, window=4)},
        )
    )
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            r = await c.post(
                "/admin/weights",
                json={"model": "m", "alias": "latest", "weights": {"v1": 0.0, "v2": 1.0}},
            )
            assert r.status_code == 200
            r = await c.post("/infer", json={"model": "m", "prompt": "http-1", "request_id": "h-1"})
            assert r.json()["result"].startswith("m@v2#")

            r = await c.post(
                "/admin/rollback", json={"model": "m", "alias": "latest", "version": "v1"}
            )
            assert r.status_code == 200 and r.json()["revoked"] == ["v2"]
            r = await c.post("/infer", json={"model": "m", "prompt": "http-2", "request_id": "h-2"})
            assert r.json()["result"].startswith("m@v1#")

            r = await c.post(
                "/admin/breaker",
                json={"model": "m", "version": "v1", "failure_ratio": 0.6, "window": 10},
            )
            assert r.status_code == 200

            vs = (await c.get("/stats/versions")).json()
            log.info("http version stats=%s", vs)
            assert vs["m@v1"]["succeeded"] >= 1
            assert vs["m@v2"]["succeeded"] >= 1
            s = (await c.get("/stats")).json()
            assert sum(v["succeeded"] for v in vs.values()) == s["succeeded"]


def two_alias_config(
    *,
    weights: dict[str, dict[str, float]] | None = None,
    v2_latency: float = 5.0,
    fail_prompts_v2: frozenset[str] = frozenset(),
) -> GatewayConfig:
    """灰度别名 canary 两版本加权；另一个别名 fresh 只指向新版本 v2；可互相降级。"""
    return GatewayConfig(
        models={
            "m": ModelSpec(
                name="m",
                versions={
                    "v1": VersionSpec(version="v1", latency_ms=5.0),
                    "v2": VersionSpec(
                        version="v2", latency_ms=v2_latency, fail_prompts=fail_prompts_v2
                    ),
                },
                aliases={"latest": "v1", "fresh": "v2"},
                alias_weights=weights or {"canary": {"v1": 1.0, "v2": 1.0}},
                fallback=("v2", "v1"),
            ),
        },
        batch=BatchConfig(window_ms=5.0, max_batch_size=8, max_wait_ms=100.0),
        admission=AdmissionConfig(
            global_concurrency=64,
            global_queue=512,
            per_model_concurrency=64,
            per_model_queue=512,
        ),
        cache=CacheConfig(capacity=1024, enabled=True),
        retry=RetryConfig(max_attempts=2),
    )


async def test_rollback_scoped_to_target_alias():
    """回退只收敛目标别名：完全没动过的另一个别名继续按原配置承接同一版本。"""
    gw = Gateway(two_alias_config())
    await gw.start()
    try:
        # 回退前：fresh 别名承接 v2
        r_before = await gw.infer("m", "fresh-p", version_or_alias="fresh", request_id="fr-0")
        assert r_before.startswith("m@v2#")

        # canary 灰度别名回退、收敛到 v1（撤销 v2 在该别名上的份额）
        revoked = await gw.rollback("m", "canary", "v1")
        log.info("scoped rollback revoked=%s", revoked)
        assert revoked == ["v2"]
        assert gw.registry.alias_targets("m", "canary") == {"v1": 1.0}

        # 没动过的 fresh 别名照常指向 v2，新请求正常返回
        r_after = await gw.infer("m", "fresh-p2", version_or_alias="fresh", request_id="fr-1")
        assert r_after.startswith("m@v2#")
        # 被撤销的版本也没有被全局下线
        assert not gw.registry.is_retired("m", "v2")
        r_exp = await gw.infer("m", "fresh-p3", version_or_alias="v2", request_id="fr-2")
        assert r_exp.startswith("m@v2#")
        # 目标别名本身已收敛到 v1
        r_canary = await gw.infer("m", "cp", version_or_alias="canary", request_id="cn-0")
        assert r_canary.startswith("m@v1#")
    finally:
        await gw.shutdown()


async def test_fallback_chain_still_works_after_rollback():
    """回退后保留版本出现后端故障，仍按降级链降级到被撤销但仍可用的版本。"""
    gw = Gateway(two_alias_config())
    await gw.start()
    try:
        await gw.rollback("m", "canary", "v1")  # canary 收敛到 v1，v2 被该别名撤销
        gw.backends[("m", "v1")].transient_failures = 10_000  # v1 持续整体故障
        result = await gw.infer("m", "need-fb", version_or_alias="canary", request_id="fb-0")
        log.info("post-rollback fallback result=%s", result)
        assert result.startswith("m@v2#")  # 降级链未被回退破坏，v2 仍可降级承接
        s = gw.stats.snapshot()
        assert s["fallbacks"] == 1 and s["succeeded"] == 1 and s["failed"] == 0

        # 只有降级目标也真实不可用时才明确失败
        gw.registry.retire_version("m", "v2")
        with pytest.raises(GatewayError) as ei:
            await gw.infer("m", "no-fb", version_or_alias="canary", request_id="fb-1")
        assert ei.value.reason is Reason.FALLBACK_UNAVAILABLE
    finally:
        await gw.shutdown()


async def test_duplicate_request_id_single_inference_across_versions():
    """灰度分流（确定性压到两个版本）下同一 request_id 只真实推理一次、结果一致。"""
    gw = Gateway(two_alias_config())
    await gw.start()
    try:
        # 先用两个不同别名把同一 request_id 的两次并发提交分别路由到 v1 与 v2
        gw.registry.set_alias_weights("m", "canary", {"v1": 1.0})
        gw.registry.set_alias_weights("m", "fresh", {"v2": 1.0})
        b1 = gw.backends[("m", "v1")]
        b2 = gw.backends[("m", "v2")]
        b1.call_count = b2.call_count = 0

        results = await asyncio.gather(
            gw.infer("m", "same-thing", version_or_alias="canary", request_id="rid-x"),
            gw.infer("m", "same-thing", version_or_alias="fresh", request_id="rid-x"),
        )
        log.info("cross-version concurrent dup results=%s", results)
        assert results[0] == results[1]          # 结果一致（同一个 request_id）
        assert b1.call_count + b2.call_count == 1  # 只真实推理一次
        assert gw.stats.snapshot()["dedup_hits"] == 1

        # 权重热更新后，顺序重复提交仍只返回同一结果，不产生第二次真实推理
        gw.registry.set_alias_weights("m", "canary", {"v1": 0.0, "v2": 1.0})
        r3 = await gw.infer("m", "same-thing", version_or_alias="canary", request_id="rid-x")
        assert r3 == results[0]
        assert b1.call_count + b2.call_count == 1
        s = gw.stats.snapshot()
        assert s["real_inferences"] == 1 and s["succeeded"] == 3
    finally:
        await gw.shutdown()


async def test_rollback_purges_revoked_version_dedup_and_cache():
    """回退后旧版本此前产生的缓存与重复提交结果都不再被返回；保留版本记录仍可用。"""
    gw = Gateway(two_alias_config())
    await gw.start()
    try:
        # canary 下产生一个 v2 结果（经别名 + 去重），并重复提交命中去重
        gw.registry.set_alias_weights("m", "canary", {"v1": 0.0, "v2": 1.0})
        r0 = await gw.infer("m", "gray-p", version_or_alias="canary", request_id="gid")
        assert r0.startswith("m@v2#")
        r0b = await gw.infer("m", "gray-p", version_or_alias="canary", request_id="gid")
        assert r0b == r0
        calls_before = gw.backends[("m", "v2")].call_count
        cache_before = len(gw._cache)

        revoked = await gw.rollback("m", "canary", "v1")
        assert revoked == ["v2"]
        assert len(gw._cache) == cache_before - 1  # v2 缓存被清除

        # 同一 request_id 经回退后的别名重新提交：旧 v2 去重结果不再命中，
        # 在 v1 上真实重新执行，结果来自 v1
        r1 = await gw.infer("m", "gray-p", version_or_alias="canary", request_id="gid")
        assert r1.startswith("m@v1#") and r1 != r0
        assert gw.backends[("m", "v2")].call_count == calls_before  # v2 未再被调用
        s = gw.stats.snapshot()
        assert s["real_inferences"] == 2  # v2 一次 + 回退后 v1 一次
    finally:
        await gw.shutdown()


async def test_inflight_sealed_after_rollback_duplicate_reexecutes():
    """回退瞬间：已在等待的跟随者照常拿到在途结果收尾；回退后的重复提交重新执行。"""
    gw = Gateway(two_alias_config(v2_latency=150.0))
    await gw.start()
    try:
        gw.registry.set_alias_weights("m", "canary", {"v1": 0.0, "v2": 1.0})
        leader = asyncio.create_task(
            gw.infer("m", "seal-p", version_or_alias="canary", request_id="seal-id")
        )
        follower = asyncio.create_task(
            gw.infer("m", "seal-p", version_or_alias="canary", request_id="seal-id")
        )
        await asyncio.sleep(0.05)  # 两者都已在途（follower 已挂上领导者 future）
        await gw.rollback("m", "canary", "v1")
        r_leader, r_follower = await asyncio.gather(leader, follower)
        log.info("sealed inflight leader=%s follower=%s", r_leader, r_follower)
        assert r_leader.startswith("m@v2#")      # 在途请求照常收尾
        assert r_follower == r_leader             # 已在等待的跟随者同样收尾

        # 回退之后才提交的同一 request_id：旧在途结果被封闭，必须重新在 v1 执行
        r_after = await gw.infer(
            "m", "seal-p", version_or_alias="canary", request_id="seal-id"
        )
        assert r_after.startswith("m@v1#") and r_after != r_leader
        s = gw.stats.snapshot()
        assert s["accepted"] == 3 and s["succeeded"] == 3
        assert s["real_inferences"] == 2         # 回退前 v2 一次 + 回退后 v1 一次
        assert s["dedup_hits"] == 1              # 只有回退前的跟随者算去重命中
    finally:
        await gw.shutdown()


async def test_random_weighted_concurrent_duplicates_execute_once():
    """随机加权分流下并发重复提交同一 request_id，即使落到不同版本也只执行一次。"""
    gw = Gateway(two_alias_config(weights={"canary": {"v1": 1.0, "v2": 1.0}}))
    await gw.start()
    try:
        # 只给同一 request_id 去重；其余请求用各自独立 id 充分走两个版本
        other = await asyncio.gather(
            *(gw.infer("m", f"warm-{i}", version_or_alias="canary",
                       request_id=f"warm-{i}") for i in range(20))
        )
        assert any(r.startswith("m@v1#") for r in other)
        assert any(r.startswith("m@v2#") for r in other)

        b1, b2 = gw.backends[("m", "v1")], gw.backends[("m", "v2")]
        b1.call_count = b2.call_count = 0
        dups = await asyncio.gather(
            *(gw.infer("m", "dup-across-versions", version_or_alias="canary",
                       request_id="dup-rid") for _ in range(12))
        )
        log.info("random-weighted dup results=%s", set(dups))
        assert len(set(dups)) == 1                    # 结果全部一致
        assert b1.call_count + b2.call_count == 1     # 只真实推理一次
        s = gw.stats.snapshot()
        assert s["real_inferences"] >= 1
    finally:
        await gw.shutdown()


async def test_inflight_finishes_and_stats_conserve_after_rollback():
    """回退瞬间在途请求照常收尾；收尾后分版本统计与整体统计守恒。"""
    gw = Gateway(two_alias_config(v2_latency=150.0))
    await gw.start()
    try:
        gw.registry.set_alias_weights("m", "canary", {"v1": 0.0, "v2": 1.0})
        # 一个在途 v2 请求（目标版本随后会被 canary 回退撤销）
        inflight = asyncio.create_task(
            gw.infer("m", "slow-x", version_or_alias="canary", request_id="slow-1")
        )
        # 一个在途 v1 请求（保留版本）
        keep = asyncio.create_task(
            gw.infer("m", "slow-y", version_or_alias="v1", request_id="slow-2")
        )
        await asyncio.sleep(0.05)
        revoked = await gw.rollback("m", "canary", "v1")
        assert revoked == ["v2"]
        ri, rk = await asyncio.gather(inflight, keep)
        log.info("inflight after rollback: revoked-route=%s kept=%s", ri, rk)
        assert ri.startswith("m@v2#")  # 已完成路由解析，照常以 v2 收尾
        assert rk.startswith("m@v1#")

        # 在途收尾后再打若干新请求（canary 全部 v1；fresh 仍 v2）
        await asyncio.gather(
            *(gw.infer("m", f"p-{i}", version_or_alias="canary", request_id=f"p-{i}")
              for i in range(8)),
            *(gw.infer("m", f"q-{i}", version_or_alias="fresh", request_id=f"q-{i}")
              for i in range(8)),
        )
        s = gw.stats.snapshot()
        vs = gw.version_stats_snapshot()
        log.info("post-rollback conservation overall=%s versions=%s", s, vs)
        assert s["accepted"] == s["succeeded"] + s["failed"] + s["rejected"]
        assert sum(v["requests"] for v in vs.values()) == s["accepted"]
        for name, v in vs.items():
            assert v["requests"] == v["succeeded"] + v["failed"] + v["rejected"], name
        for key in ("succeeded", "failed", "rejected", "real_inferences", "fallbacks"):
            assert sum(v[key] for v in vs.values()) == s[key], key
    finally:
        await gw.shutdown()
