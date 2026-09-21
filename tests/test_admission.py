"""背压与资源隔离：排队超限明确拒绝；慢模型不阻塞其他模型。"""
from __future__ import annotations

import asyncio
import time

from gateway.config import AdmissionConfig
from gateway.errors import GatewayError, Reason
from gateway.gateway import Gateway
from tests.conftest import log, make_config


async def test_queue_full_rejection():
    """慢模型占满并发槽后，超出排队上限的请求以 QUEUE_FULL 拒绝。"""
    cfg = make_config(admission=AdmissionConfig(
        global_concurrency=4, global_queue=8,
        per_model_concurrency=2, per_model_queue=2, wait_timeout_ms=50.0,
    ))
    gw = Gateway(cfg)
    await gw.start()
    try:
        # 2 个占槽 + 2 个排队 + 第 5 个应被拒绝
        tasks = [asyncio.create_task(gw.infer("slow", f"q-{i}", request_id=f"rq-{i}"))
                 for i in range(5)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        reasons = [r.reason if isinstance(r, GatewayError) else "ok" for r in results]
        log.info("queue full: reasons=%s", reasons)
        assert Reason.QUEUE_FULL in reasons or Reason.WAIT_TIMEOUT in reasons
        ok = [r for r in results if isinstance(r, str)]
        assert len(ok) >= 2  # 至少在槽内的请求成功完成
        s = gw.stats.snapshot()
        log.info("stats=%s", s)
        assert s["accepted"] == s["succeeded"] + s["failed"] + s["rejected"]
        assert s["rejected"] >= 1
    finally:
        await gw.shutdown()


async def test_slow_model_does_not_block_fast_model():
    """慢模型占满自己的并发槽时，快模型仍可即时完成。"""
    cfg = make_config(admission=AdmissionConfig(
        global_concurrency=8, global_queue=16,
        per_model_concurrency=2, per_model_queue=4, wait_timeout_ms=1000.0,
    ))
    gw = Gateway(cfg)
    await gw.start()
    try:
        slow_tasks = [asyncio.create_task(gw.infer("slow", f"s-{i}", request_id=f"rs-{i}"))
                      for i in range(2)]
        await asyncio.sleep(0.05)  # 确保慢请求已占槽
        t0 = time.monotonic()
        fast = await gw.infer("m", "fast", request_id="r-fast")
        elapsed = time.monotonic() - t0
        log.info("fast req=r-fast result=%s elapsed=%.3fs", fast, elapsed)
        assert fast.startswith("m@v1#")
        assert elapsed < 0.15  # 远小于慢模型 150ms 延迟
        await asyncio.gather(*slow_tasks)
    finally:
        await gw.shutdown()
