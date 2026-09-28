"""一键回退作用域、灰度重复提交幂等与回退后统计守恒的回归测试。

覆盖问题：
- 回退某个别名不能牵连其它别名，也不能破坏重试/降级链路；
- 灰度加权分流、权重热更新、回退后，同一 request_id 只真实推理一次且结果一致；
- 回退撤下的版本此前产生的缓存/去重结果不再被返回；
- 在途请求收尾后，分版本统计与整体统计守恒。
"""
from __future__ import annotations

import asyncio

import pytest

from gateway.config import (
    AdmissionConfig,
    BatchConfig,
    CacheConfig,
    GatewayConfig,
    ModelSpec,
    RetryConfig,
    VersionSpec,
)
from gateway.errors import GatewayError, Reason
from gateway.gateway import Gateway
from gateway.registry import Registry
from tests.conftest import log


def two_alias_config(*, v2_latency: float = 5.0) -> GatewayConfig:
    """两个别名：canary 在 v1/v2 间灰度；stable 固定指向 v2；降级链 v1 -> v2。"""
    return GatewayConfig(
        models={
            "m": ModelSpec(
                name="m",
                versions={
                    "v1": VersionSpec(version="v1", latency_ms=5.0),
                    "v2": VersionSpec(version="v2", latency_ms=v2_latency),
                },
                aliases={"latest": "v1", "stable": "v2"},
                alias_weights={"canary": {"v1": 1.0, "v2": 1.0}},
                fallback=("v2",),
            ),
        },
        batch=BatchConfig(window_ms=5.0, max_batch_size=8, max_wait_ms=100.0),
        admission=AdmissionConfig(),
        cache=CacheConfig(capacity=1024, enabled=True),
        retry=RetryConfig(max_attempts=2),
    )


def patch_pick(gw: Gateway, sequence: list[str]) -> list[int]:
    """确定性替换加权选择：依次返回 sequence 中的版本；返回调用次数记录器。"""
    calls = [0]

    def fake_pick(self: Registry, model: str, alias: str) -> str:
        v = sequence[calls[0] % len(sequence)]
        calls[0] += 1
        return v

    gw.registry._pick_weighted = fake_pick.__get__(gw.registry, Registry)  # type: ignore[method-assign]
    return calls


async def test_rollback_scoped_to_target_alias():
    """回退 canary 到 v1 后，未触碰的 stable 别名仍正常由 v2 承接。"""
    gw = Gateway(two_alias_config())
    await gw.start()
    try:
        r_stable_before = await gw.infer("m", "s-1", version_or_alias="stable",
                                        request_id="s-1")
        assert r_stable_before.startswith("m@v2#")

        revoked = await gw.rollback("m", "canary", "v1")
        log.info("rollback canary->v1 revoked=%s", revoked)
        assert revoked == ["v2"]
        assert gw.registry.alias_targets("m", "canary") == {"v1": 1.0}

        # stable 完全没动过：权重组成与承接版本保持回退前状态
        assert gw.registry.alias_targets("m", "stable") == {"v2": 1.0}
        r_stable_after = await asyncio.gather(
            *(gw.infer("m", f"sa-{i}", version_or_alias="stable",
                       request_id=f"sa-{i}") for i in range(8))
        )
        assert all(r.startswith("m@v2#") for r in r_stable_after)
        # canary 新请求全部收敛到 v1
        r_canary = await asyncio.gather(
            *(gw.infer("m", f"c-{i}", version_or_alias="canary",
                       request_id=f"c-{i}") for i in range(8))
        )
        assert all(r.startswith("m@v1#") for r in r_canary)
    finally:
        await gw.shutdown()


async def test_rollback_keeps_fallback_chain_working():
    """回退后保留版本后端故障时，仍按配置降级到另一可用版本并成功返回。"""
    gw = Gateway(two_alias_config())
    await gw.start()
    try:
        assert await gw.rollback("m", "canary", "v1") == ["v2"]
        # 回退撤下的 v2 并未下线：让保留版本 v1 持续整体故障
        gw.backends[("m", "v1")].transient_failures = 10_000
        results = await asyncio.gather(
            *(gw.infer("m", f"fb-{i}", version_or_alias="canary",
                       request_id=f"fb-{i}") for i in range(5)),
            return_exceptions=True,
        )
        log.info("post-rollback fallback results=%s", results)
        assert all(isinstance(r, str) and r.startswith("m@v2#") for r in results)
        s = gw.stats.snapshot()
        assert s["fallbacks"] == 5 and s["failed"] == 0
        assert s["accepted"] == s["succeeded"] + s["failed"] + s["rejected"]

        # 降级目标确实不可用时才允许明确失败（v2 被真正下线后）
        gw.registry.retire_version("m", "v2")
        with pytest.raises(GatewayError) as ei:
            await gw.infer("m", "fb-dead", version_or_alias="canary",
                           request_id="fb-dead")
        assert ei.value.reason is Reason.FALLBACK_UNAVAILABLE
    finally:
        await gw.shutdown()


async def test_concurrent_same_request_id_under_split_executes_once():
    """灰度下同一 request_id 并发提交（落到不同版本）只真实推理一次、结果一致。"""
    gw = Gateway(two_alias_config())
    await gw.start()
    try:
        # 即使加权选择依次给出不同版本，去重也必须保证只执行一次
        calls = patch_pick(gw, ["v1", "v2", "v2", "v1"])
        gw.backends[("m", "v1")].call_count = 0
        gw.backends[("m", "v2")].call_count = 0

        results = await asyncio.gather(
            *(gw.infer("m", "gray-dup", version_or_alias="canary",
                       request_id="rid-gray") for _ in range(8))
        )
        log.info("gray split dup picks=%d results=%s", calls[0], set(results))
        assert len(set(results)) == 1                 # 结果完全一致
        assert results[0].startswith("m@v1#")        # 以第一次解析的版本执行
        total_calls = (gw.backends[("m", "v1")].call_count
                       + gw.backends[("m", "v2")].call_count)
        assert total_calls == 1                       # 跨版本合计只真实推理一次
        s = gw.stats.snapshot()
        assert s["real_inferences"] == 1
        assert s["dedup_hits"] == 7
        assert s["accepted"] == s["succeeded"] + s["failed"] + s["rejected"]
    finally:
        await gw.shutdown()


async def test_same_request_id_consistent_after_weight_hot_update():
    """权重热更新后重复提交同一 request_id，仍返回第一次的结果，不二次推理。"""
    gw = Gateway(two_alias_config())
    await gw.start()
    try:
        patch_pick(gw, ["v1", "v2"])
        r1 = await gw.infer("m", "hot-dup", version_or_alias="canary",
                            request_id="rid-hot")
        assert r1.startswith("m@v1#")
        # 热更新：canary 全量切到 v2
        gw.registry.set_alias_weights("m", "canary", {"v1": 0.0, "v2": 1.0})
        gw.backends[("m", "v1")].call_count = 0
        gw.backends[("m", "v2")].call_count = 0
        r2 = await gw.infer("m", "hot-dup", version_or_alias="canary",
                            request_id="rid-hot")
        log.info("hot-update dup r1=%s r2=%s", r1, r2)
        assert r2 == r1                               # 不返回 v2 的不同结果
        assert gw.backends[("m", "v1")].call_count == 0
        assert gw.backends[("m", "v2")].call_count == 0
    finally:
        await gw.shutdown()


async def test_rolled_back_version_results_never_hit_after():
    """回退后旧版本的缓存与重复提交结果都不再命中；保留版本记录仍可用。"""
    gw = Gateway(two_alias_config())
    await gw.start()
    try:
        old = await gw.infer("m", "will-roll", version_or_alias="v2",
                             request_id="rid-old")
        kept = await gw.infer("m", "will-keep", version_or_alias="v1",
                              request_id="rid-keep")
        assert old.startswith("m@v2#") and kept.startswith("m@v1#")
        assert await gw.rollback("m", "canary", "v1") == ["v2"]

        # 同 request_id 经（已收敛的）别名重提：不得再返回 v2 旧结果
        r = await gw.infer("m", "will-roll", version_or_alias="canary",
                           request_id="rid-old")
        log.info("post-rollback re-submit old=%s", r)
        assert r.startswith("m@v1#")
        # 同 prompt 的 v2 缓存也已清除：经别名执行得到 v1 结果
        r2 = await gw.infer("m", "will-roll", version_or_alias="canary",
                            request_id="rid-old-2")
        assert r2 == r
        # 保留版本 v1 的去重记录仍命中
        r_keep = await gw.infer("m", "will-keep", version_or_alias="canary",
                                request_id="rid-keep")
        assert r_keep == kept

        # 回退是别名作用域：显式访问 v2 仍可用；但旧去重记录已清除，
        # 用全新 request_id 显式访问 v2 必须真实推理（且 stable 别名不受影响）。
        gw.backends[("m", "v2")].call_count = 0
        r_exp = await gw.infer("m", "will-roll", version_or_alias="v2",
                               request_id="rid-explicit-v2")
        assert r_exp.startswith("m@v2#")
        assert gw.backends[("m", "v2")].call_count == 1
    finally:
        await gw.shutdown()


async def test_inflight_finish_and_stats_conservation_after_rollback():
    """回退时在途请求照常收尾；收尾后分版本统计与整体统计守恒。"""
    gw = Gateway(two_alias_config(v2_latency=150.0))
    await gw.start()
    try:
        inflight = [
            asyncio.create_task(
                gw.infer("m", f"slow-{i}", version_or_alias="v2",
                         request_id=f"slow-{i}")
            )
            for i in range(4)
        ]
        await asyncio.sleep(0.05)
        revoked = await gw.rollback("m", "canary", "v1")
        assert revoked == ["v2"]
        slow_results = await asyncio.gather(*inflight)  # 在途请求正常结束
        assert all(r.startswith("m@v2#") for r in slow_results)

        # 未参与回退的 stable 别名仍由 v2 承接（旧实现会在此处失败）
        r_stable = await gw.infer("m", "stable-ok", version_or_alias="stable",
                                  request_id="stable-ok")
        assert r_stable.startswith("m@v2#")

        # 回退后再打一批 v1 流量
        await asyncio.gather(
            *(gw.infer("m", f"new-{i}", version_or_alias="canary",
                       request_id=f"new-{i}") for i in range(6))
        )
        s = gw.stats.snapshot()
        vs = gw.version_stats_snapshot()
        log.info("post-rollback conservation overall=%s versions=%s", s, vs)
        assert s["accepted"] == s["succeeded"] + s["failed"] + s["rejected"]
        assert sum(v["requests"] for v in vs.values()) == s["accepted"]
        for key in ("succeeded", "failed", "rejected", "real_inferences"):
            assert sum(v[key] for v in vs.values()) == s[key], key
        for name, v in vs.items():
            assert v["requests"] == v["succeeded"] + v["failed"] + v["rejected"], name
        assert vs["m@v2"]["succeeded"] == 5  # 4 在途 + 1 stable
        assert vs["m@v1"]["succeeded"] == 6
    finally:
        await gw.shutdown()
