"""灰度放量：权重分流、热更新一致性、自动止损、一键回退与分版本统计守恒。"""
from __future__ import annotations

import asyncio

import pytest

from gateway.config import (
    AdmissionConfig,
    BatchConfig,
    BreakerSpec,
    CacheConfig,
    GatewayConfig,
    ModelSpec,
    RetryConfig,
    VersionSpec,
)
from gateway.errors import GatewayError, Reason
from gateway.gateway import Gateway
from tests.conftest import log, make_config


def rollout_config(
    *,
    breaker: BreakerSpec | None = None,
    v2_fail_prompts: frozenset[str] = frozenset(),
    v2_latency_ms: float = 5.0,
) -> GatewayConfig:
    """单模型两版本、无降级链、不重试，便于精确计数。"""
    return GatewayConfig(
        models={
            "m": ModelSpec(
                name="m",
                versions={
                    "v1": VersionSpec(version="v1", latency_ms=5.0),
                    "v2": VersionSpec(
                        version="v2",
                        latency_ms=v2_latency_ms,
                        fail_prompts=v2_fail_prompts,
                        breaker=breaker,
                    ),
                },
                aliases={"latest": "v1"},
            ),
        },
        batch=BatchConfig(window_ms=5.0, max_batch_size=8, max_wait_ms=200.0),
        admission=AdmissionConfig(
            global_concurrency=64,
            global_queue=1024,
            per_model_concurrency=64,
            per_model_queue=1024,
        ),
        cache=CacheConfig(capacity=1024, enabled=True),
        retry=RetryConfig(max_attempts=1),
    )


@pytest.fixture
async def rgw():
    g = Gateway(rollout_config())
    await g.start()
    yield g
    await g.shutdown()


def version_of(result: str) -> str:
    """结果必须且只能归属一个版本。"""
    assert result.startswith("m@"), f"unexpected result: {result}"
    return result.split("@")[1].split("#")[0]


async def test_weighted_distribution_matches_config(rgw):
    """多版本分流下，足够多的独立请求体现接近配置权重的分布。"""
    rgw.registry.set_alias_weights("m", "latest", {"v1": 1.0, "v2": 3.0})
    n = 400
    results = await asyncio.gather(
        *(rgw.infer("m", f"dist-{i}", request_id=f"rd-{i}") for i in range(n))
    )
    versions = [version_of(r) for r in results]
    assert set(versions) <= {"v1", "v2"}  # 单请求只归属一个版本
    snap = rgw.monitor.snapshot()
    v2_ratio = snap["m@v2"]["requests"] / n
    log.info("weights=1:3 n=%d v2_ratio=%.3f snap=%s", n, v2_ratio, snap)
    assert snap["m@v1"]["requests"] + snap["m@v2"]["requests"] == n
    assert 0.65 < v2_ratio < 0.85  # 期望 0.75


async def test_concurrent_weight_hot_update(rgw):
    """并发下热更新权重：更新后新请求按最新配置分流，权重 0 的版本不再接新请求。"""
    rgw.registry.set_alias_weights("m", "latest", {"v1": 1.0, "v2": 0.0})
    before = await asyncio.gather(
        *(rgw.infer("m", f"hot-a-{i}", request_id=f"rh-a-{i}") for i in range(10))
    )
    assert all(version_of(r) == "v1" for r in before)

    # 热更新：v1 权重归零，全部切到 v2
    rgw.registry.set_alias_weights("m", "latest", {"v1": 0.0, "v2": 1.0})
    after = await asyncio.gather(
        *(rgw.infer("m", f"hot-b-{i}", request_id=f"rh-b-{i}") for i in range(10))
    )
    snap = rgw.monitor.snapshot()
    log.info("after hot update: results=%s snap=%s", {version_of(r) for r in after}, snap)
    assert all(version_of(r) == "v2" for r in after)
    assert snap["m@v1"]["requests"] == 10  # 更新后 v1 没有新请求
    assert snap["m@v2"]["requests"] == 10


async def test_inflight_unaffected_by_weight_update():
    """权重更新不影响在途请求：在途请求按进入时的版本正常完成。"""
    gw = Gateway(rollout_config(v2_latency_ms=100.0))
    await gw.start()
    try:
        gw.registry.set_alias_weights("m", "latest", {"v1": 0.0, "v2": 1.0})
        inflight = asyncio.create_task(gw.infer("m", "slow", request_id="rh-inflight"))
        await asyncio.sleep(0.02)  # 确认请求已进入 v2 并在途
        gw.registry.set_alias_weights("m", "latest", {"v1": 1.0, "v2": 0.0})
        result = await inflight  # 在途请求正常收尾，不中途换版本
        log.info("req=rh-inflight result=%s", result)
        assert version_of(result) == "v2"
        new = await gw.infer("m", "new", request_id="rh-new")
        assert version_of(new) == "v1"
    finally:
        await gw.shutdown()


async def test_breaker_halts_failing_version_inflight_finishes():
    """自动止损：失败比例达阈值即停止该版本放量；在途请求正常收尾、计数不错乱。"""
    gw = Gateway(
        rollout_config(
            breaker=BreakerSpec(failure_ratio=0.5, window=4, min_requests=4),
            v2_fail_prompts=frozenset({"boom"}),
            v2_latency_ms=100.0,
        )
    )
    await gw.start()
    try:
        gw.registry.set_alias_weights("m", "latest", {"v1": 0.0, "v2": 1.0})
        # 先发 4 个必败请求，再发 1 个慢请求——止损触发时慢请求仍在途
        booms = [
            asyncio.create_task(gw.infer("m", "boom", request_id=f"rb-boom-{i}"))
            for i in range(4)
        ]
        await asyncio.sleep(0.02)
        inflight = asyncio.create_task(gw.infer("m", "slow-ok", request_id="rb-inflight"))

        boom_results = await asyncio.gather(*booms, return_exceptions=True)
        assert all(
            isinstance(r, GatewayError) and r.reason is Reason.BACKEND_ERROR
            for r in boom_results
        )
        assert gw.registry.is_halted("m", "v2")  # 止损已触发

        result = await inflight  # 在途请求正常结束，结果仍来自 v2，无半更新
        log.info("req=rb-inflight result=%s halted=v2", result)
        assert version_of(result) == "v2"

        # 止损后：其余版本继续正常服务，被止损版本不再接新请求
        gw.registry.set_alias_weights("m", "latest", {"v1": 1.0, "v2": 1.0})
        after = await asyncio.gather(
            *(gw.infer("m", f"after-{i}", request_id=f"rb-after-{i}") for i in range(10))
        )
        assert all(version_of(r) == "v1" for r in after)

        # 分版本计数与整体口径一致
        snap = gw.monitor.snapshot()
        s = gw.stats.snapshot()
        log.info("snap=%s stats=%s", snap, s)
        assert snap["m@v2"]["requests"] == 5
        assert snap["m@v2"]["failed"] == 4
        assert snap["m@v2"]["succeeded"] == 1
        assert snap["m@v1"]["requests"] == 10
        assert snap["m@v1"]["succeeded"] == 10
        total = snap["m@v1"]["succeeded"] + snap["m@v2"]["succeeded"]
        assert total == s["succeeded"]
        assert snap["m@v2"]["failed"] == s["failed"]
    finally:
        await gw.shutdown()


async def test_rollback_revokes_old_version_results():
    """一键回退：新请求不再进入被撤销版本；其缓存与重复提交结果不再被返回。"""
    gw = Gateway(rollout_config(v2_latency_ms=100.0))
    await gw.start()
    try:
        gw.registry.set_alias_weights("m", "latest", {"v1": 0.0, "v2": 1.0})
        r1 = await gw.infer("m", "hello", request_id="rr-1")  # v2 产出，入缓存与去重表
        assert version_of(r1) == "v2"
        before = gw.stats.snapshot()["real_inferences"]

        # 在途请求 + 回退并发：回退不影响在途请求
        inflight = asyncio.create_task(gw.infer("m", "slow", request_id="rr-inflight"))
        await asyncio.sleep(0.02)
        revoked = await gw.rollback("m", "latest", "v1")
        log.info("rollback revoked=%s", revoked)
        assert revoked == ["v2"]
        assert version_of(await inflight) == "v2"  # 在途请求正常收尾

        # 回退后新请求只走 v1
        r2 = await gw.infer("m", "hello", request_id="rr-2")
        assert version_of(r2) == "v1"

        # 重复提交旧 request_id：不再返回 v2 的去重结果，而是真实重新执行
        r1_again = await gw.infer("m", "hello", request_id="rr-1")
        after = gw.stats.snapshot()
        log.info("req=rr-1 again result=%s real_inferences=%d", r1_again, after["real_inferences"])
        assert version_of(r1_again) == "v1"
        assert after["real_inferences"] == before + 2  # rr-2 与 rr-1 重算各一次
        assert after["dedup_hits"] == 0
    finally:
        await gw.shutdown()


async def test_rollback_conserves_stats_and_cache_bound():
    """回退不破坏缓存容量上限与统计守恒。"""
    gw = Gateway(make_config())  # cache capacity=4
    await gw.start()
    try:
        for i in range(6):  # 超出容量，验证 LRU 上限仍然成立
            await gw.infer("m", f"cap-{i}", request_id=f"rc-{i}")
        await gw.rollback("m", "latest", "v2")
        for i in range(6, 10):
            await gw.infer("m", f"cap-{i}", request_id=f"rc-{i}")
        s = gw.stats.snapshot()
        log.info("cache_len=%d stats=%s", len(gw._cache), s)
        assert len(gw._cache) <= 4  # 容量上限不因回退失效
        assert s["accepted"] == s["succeeded"] + s["failed"] + s["rejected"]
    finally:
        await gw.shutdown()


async def test_version_stats_conservation_under_concurrency():
    """分版本统计与整体统计守恒：并发 + 重复提交 + 降级混合场景。"""
    gw = Gateway(make_config())  # 含降级链 v1->v2、重试 2 次、cache capacity=4
    await gw.start()
    try:
        gw.backends[("m", "v1")].transient_failures = 4  # 部分请求将降级到 v2
        tasks = [
            gw.infer("m", f"p-{i}", request_id=f"rq-{i}") for i in range(8)
        ]
        tasks.append(gw.infer("m", "p-0", request_id="rq-0"))  # 在途重复提交（去重）
        tasks.append(gw.infer("m", "p-1", request_id="rq-1b"))  # 同 prompt 不同 id
        results = await asyncio.gather(*tasks, return_exceptions=True)
        assert not [r for r in results if isinstance(r, Exception)]  # 降级兜底，全部成功
        again = await gw.infer("m", "p-0", request_id="rq-0")  # 已完成去重命中
        assert again == results[0]

        s = gw.stats.snapshot()
        snap = gw.monitor.snapshot()
        log.info("stats=%s versions=%s", s, snap)
        # 整体守恒
        assert s["accepted"] == s["succeeded"] + s["failed"] + s["rejected"]
        # 分版本合计与整体口径一致
        for key in ("succeeded", "failed", "rejected", "fallbacks", "real_inferences"):
            assert sum(v[key] for v in snap.values()) == s[key], key
        # 每版本：承接 = 成功 + 失败 + 拒绝；合计 = 整体 accepted（本场景无路由前拒绝）
        for name, v in snap.items():
            assert v["requests"] == v["succeeded"] + v["failed"] + v["rejected"], name
        assert sum(v["requests"] for v in snap.values()) == s["accepted"]
        # 降级确实发生且推理归属到实际产出结果的 v2
        assert s["fallbacks"] > 0
        assert snap["m@v2"]["real_inferences"] == s["real_inferences"]
    finally:
        await gw.shutdown()
