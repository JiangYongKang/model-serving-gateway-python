"""去重与用量统计：相同请求标识只真实推理一次；并发/失败/降级下统计守恒。"""
from __future__ import annotations

import asyncio

from gateway.gateway import Gateway
from tests.conftest import log, make_config


async def test_concurrent_duplicate_request_id_single_inference():
    gw = Gateway(make_config())
    await gw.start()
    try:
        backend = gw.backends[("m", "v1")]
        backend.call_count = 0
        results = await asyncio.gather(
            *(gw.infer("m", "dup", request_id="rd-same") for _ in range(6))
        )
        s = gw.stats.snapshot()
        log.info("dedup: results=%s calls=%d stats=%s", set(results), backend.call_count, s)
        assert len(set(results)) == 1          # 结果一致
        assert backend.call_count == 1         # 只真实推理一次
        assert s["real_inferences"] == 1
        assert s["dedup_hits"] == 5
        assert s["succeeded"] == 6
        assert s["accepted"] == s["succeeded"] + s["failed"] + s["rejected"]
    finally:
        await gw.shutdown()


async def test_sequential_duplicate_uses_recorded_result():
    gw = Gateway(make_config())
    await gw.start()
    try:
        backend = gw.backends[("m", "v1")]
        backend.call_count = 0
        r1 = await gw.infer("m", "seq-dup", request_id="rd-seq")
        r2 = await gw.infer("m", "seq-dup", request_id="rd-seq")
        s = gw.stats.snapshot()
        log.info("seq dedup: r1=%s r2=%s calls=%d stats=%s", r1, r2, backend.call_count, s)
        assert r1 == r2
        assert backend.call_count == 1
        assert s["real_inferences"] == 1
    finally:
        await gw.shutdown()


async def test_stats_conservation_under_mixed_outcomes():
    gw = Gateway(make_config(fail_prompts=frozenset({"bad"})))
    await gw.start()
    try:
        results = await asyncio.gather(
            gw.infer("m", "ok-1", request_id="rm-1"),
            gw.infer("m", "ok-1", request_id="rm-1"),              # 重复提交 -> 去重
            gw.infer("m", "bad", request_id="rm-2"),               # 单项确定性失败
            gw.infer("nope", "x", request_id="rm-4"),              # 未知模型
            return_exceptions=True,
        )
        s = gw.stats.snapshot()
        log.info("mixed: results=%s stats=%s", results, s)
        assert s["accepted"] == 4
        assert s["accepted"] == s["succeeded"] + s["failed"] + s["rejected"]
        assert s["succeeded"] == 2      # rm-1 两次提交均成功（一次真实推理）
        assert s["failed"] == 1         # bad
        assert s["rejected"] == 1       # 未知模型
        assert s["dedup_hits"] == 1
        assert s["real_inferences"] == 2  # ok-1 一次 + bad 一次
    finally:
        await gw.shutdown()
