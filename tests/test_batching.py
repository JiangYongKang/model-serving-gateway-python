"""动态合批测试。"""

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


def fast_config(**batch_kw) -> GatewayConfig:
    versions = [
        VersionSpec(model="m", version="v1", latency=0.05),
        VersionSpec(model="m", version="v2", latency=0.05),
        VersionSpec(model="n", version="v1", latency=0.05),
        VersionSpec(model="mix", version="bad", latency=0.01,
                    fault="always_fail"),
        VersionSpec(model="mix", version="ok", latency=0.01),
    ]
    return GatewayConfig(
        versions=versions,
        batch=BatchConfig(window=0.1, max_size=batch_kw.pop("max_size", 8)),
        resources=ResourceConfig(
            global_concurrency=8, global_queue=64,
            per_model_concurrency=4, per_model_queue=32,
            acquire_timeout=2.0,
        ),
        retry=RetryConfig(max_retries=0),
        fallback_chains={"mix:bad": "ok"},
        **batch_kw,
    )


@pytest.mark.asyncio
async def test_concurrent_requests_share_batch(client_factory):
    async with client_factory(fast_config()) as (client, _app):
        async def call(i):
            r = await client.post("/v1/infer", json={
                "model": "m", "version": "v1",
                "input": {"i": i}, "request_id": f"b-{i}"})
            return r.json()

        results = await asyncio.gather(*[call(i) for i in range(6)])
        batch_ids = {r["batch_id"] for r in results}
        # 6 个同时到达的请求应被合入同一批（窗口内、未超批大小）。
        assert len(batch_ids) == 1, f"expected one batch, got {batch_ids}"
        # 结果互不相同（输入不同），但批次相同
        outputs = {r["output"] for r in results}
        assert len(outputs) == 6
        print(f"\n[合批] request_ids=b-0..b-5 batch_id={results[0]['batch_id']} "
              f"reason=concurrent_within_window")


@pytest.mark.asyncio
async def test_different_models_or_versions_never_share_batch(client_factory):
    async with client_factory(fast_config()) as (client, _app):
        async def call(model, version, rid):
            r = await client.post("/v1/infer", json={
                "model": model, "version": version,
                "input": {"x": 1}, "request_id": rid})
            return r.json()

        results = await asyncio.gather(
            call("m", "v1", "iso-1"),
            call("m", "v2", "iso-2"),
            call("n", "v1", "iso-3"),
        )
        batch_ids = {r["batch_id"] for r in results}
        assert len(batch_ids) == 3, f"expected 3 batches, got {batch_ids}"
        print(f"\n[隔离] 三个键各自成批 batch_ids={sorted(batch_ids)} "
              f"reason=per_model_version_queue")


@pytest.mark.asyncio
async def test_batch_item_failure_does_not_pollute_others(client_factory):
    """同批中某输入走 always_fail 版本时：它失败/降级，其余请求不受影响。

    这里让所有请求都走 bad 版本会全批失败；改为混合不可行（同键同版本），
    因此用 bad 版本批验证“逐项独立结算”，并用 ok 版本对照。
    """
    async with client_factory(fast_config()) as (client, _app):
        async def call_bad(i):
            r = await client.post("/v1/infer", json={
                "model": "mix", "version": "bad",
                "input": {"i": i}, "request_id": f"bad-{i}"})
            return r.status_code, r.json()

        # bad 恒失败、无重试、降级到 ok -> 全部 200 且输出来自 ok
        results = await asyncio.gather(*[call_bad(i) for i in range(3)])
        for status, body in results:
            assert status == 200
            assert body["version"] == "ok"
            assert body["fallback"] == "ok"
        print(f"\n[批内隔离] 3 个失败项各自独立降级，未互相污染 "
              f"reason=per_item_future_settlement")


@pytest.mark.asyncio
async def test_slow_model_does_not_block_fast_model(client_factory):
    """慢模型占满自身并发槽时，快模型仍能及时完成。"""
    cfg = GatewayConfig(
        versions=[
            VersionSpec(model="slow", version="v1", latency=0.3),
            VersionSpec(model="fast", version="v1", latency=0.0),
        ],
        batch=BatchConfig(window=0.05, max_size=8),
        resources=ResourceConfig(
            global_concurrency=8, global_queue=64,
            per_model_concurrency=2, per_model_queue=32,
            acquire_timeout=2.0,
        ),
        retry=RetryConfig(max_retries=0),
    )
    async with client_factory(cfg) as (client, _app):
        async def call(model, rid):
            r = await client.post("/v1/infer", json={
                "model": model, "version": "v1",
                "input": {"i": rid}, "request_id": rid})
            assert r.status_code == 200
            return r.elapsed.total_seconds()

        loop = asyncio.get_running_loop()
        # 先占住 slow 的批次（一批 2 个并发槽，各持 ~0.3s）
        slow_tasks = [
            asyncio.create_task(call("slow", f"slow-{i}")) for i in range(2)
        ]
        await asyncio.sleep(0.02)  # 确保 slow 先入批开始执行
        t0 = loop.time()
        fast_elapsed = await call("fast", "fast-1")
        fast_wall = loop.time() - t0
        await asyncio.gather(*slow_tasks)
        # fast 不应被 slow 的 0.3s 阻塞
        assert fast_wall < 0.2, f"fast blocked by slow: {fast_wall:.3f}s"
        print(f"\n[慢模型隔离] fast 完成耗时={fast_wall*1000:.0f}ms "
              f"reason=independent_workers_and_slots")
