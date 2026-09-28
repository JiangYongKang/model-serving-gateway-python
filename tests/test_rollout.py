"""灰度放量、自动止损与一键回退。

- 加权别名分流：分布接近配置权重；权重热更新即时生效；权重 0 不再接新请求；
- 自动止损：失败比例达阈值后版本停止放量，其余版本继续服务，在途请求正常收尾；
- 一键回退：别名收敛单版本，被撤销版本的缓存/去重结果不再返回，在途请求不受影响；
- 分版本统计与整体统计守恒（并发、重复提交、降级、背压场景）。
"""
from __future__ import annotations

import asyncio

import httpx

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
        # 回退只作用于别名：被撤销版本并未全局下线，显式访问仍可用
        r_explicit = await gw.infer(
            "m", "keep-v2", version_or_alias="v2", request_id="rb-v2-exp"
        )
        log.info("explicit access after rollback result=%s", r_explicit)
        assert r_explicit.startswith("m@v2#")
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
