"""资源隔离与背压测试：队列超限快速拒绝、等待超时、积压有界。"""

from __future__ import annotations

import asyncio

import pytest

from app.config import (
    BatchConfig,
    GatewayConfig,
    ResourceConfig,
    RetryConfig,
    VersionSpec,
)


def tight_config() -> GatewayConfig:
    return GatewayConfig(
        versions=[VersionSpec(model="slow", version="v1", latency=0.3)],
        # max_size=1：每个请求独立占资源槽，才能压满并发+排队
        batch=BatchConfig(window=0.02, max_size=1, max_wait=5.0,
                          max_inflight_batches=1),
        resources=ResourceConfig(
            global_concurrency=8, global_queue=64,
            per_model_concurrency=1, per_model_queue=2,
            acquire_timeout=0.2,
        ),
        retry=RetryConfig(max_retries=0),
    )


@pytest.mark.asyncio
async def test_queue_overflow_is_rejected_with_distinct_reason(client_factory):
    cfg = tight_config()
    async with client_factory(cfg) as (client, _app):
        statuses: list[tuple[int, str]] = []

        async def call(rid):
            r = await client.post("/v1/infer", json={
                "model": "slow", "version": "v1",
                "input": {"i": rid}, "request_id": rid})
            statuses.append((r.status_code, r.json().get("error", "ok")))

        # 用 6 个并发请求冲垮：并发槽1 + 等待队列2，其余必须被明确拒绝。
        await asyncio.gather(*[call(f"bp-{i}") for i in range(6)],
                             return_exceptions=True)
        reasons = {}
        for status, reason in statuses:
            reasons[reason] = reasons.get(reason, 0) + 1
        print(f"\n[背压] 6 请求结果分布={reasons} "
              f"reason=concurrency1_queue2")
        assert "queue_full" in reasons
        # 至少有一部分被快速拒绝，不能全部积压
        assert reasons.get("queue_full", 0) >= 1
        # 每个被接收的请求要么成功要么因等待超时失败，没有无界等待
        accepted = [s for s in statuses if s[1] != "queue_full"]
        assert all(s == 200 or reason in ("wait_timeout",)
                   for s, reason in accepted)


@pytest.mark.asyncio
async def test_wait_timeout_reason(client_factory):
    # 并发1、窗口极小、逐请求错峰 -> 后续请求在信号量前等待至超时
    cfg = GatewayConfig(
        versions=[VersionSpec(model="slow", version="v1", latency=0.3)],
        batch=BatchConfig(window=0.005, max_size=1, max_wait=5.0,
                          max_inflight_batches=1),
        resources=ResourceConfig(
            global_concurrency=8, global_queue=64,
            per_model_concurrency=1, per_model_queue=4,
            acquire_timeout=0.05,
        ),
        retry=RetryConfig(max_retries=0),
    )
    async with client_factory(cfg) as (client, _app):
        results = []

        async def call(rid):
            r = await client.post("/v1/infer", json={
                "model": "slow", "version": "v1",
                "input": {"i": rid}, "request_id": rid})
            results.append((r.status_code, r.json().get("error", "ok")))

        # to-0 后台占住唯一并发槽（0.3s）；to-1 立即排队；再发 to-2/to-3，
        # acquire_timeout=0.05s 后排队者应得到 wait_timeout。
        held = [asyncio.create_task(call("to-0"))]
        await asyncio.sleep(0.02)
        held.append(asyncio.create_task(call("to-1")))
        await asyncio.sleep(0.02)
        await asyncio.gather(*[call(f"to-{i}") for i in (2, 3)])
        await asyncio.gather(*held)
        reasons = {reason for _s, reason in results}
        print(f"\n[等待超时] 结果原因集合={reasons} reason=acquire_timeout")
        assert "wait_timeout" in reasons
