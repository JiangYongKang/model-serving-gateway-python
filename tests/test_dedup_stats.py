"""请求去重与用量统计守恒测试。"""

from __future__ import annotations

import asyncio

import pytest

from app.config import (
    BatchConfig,
    CacheConfig,
    GatewayConfig,
    ResourceConfig,
    RetryConfig,
    VersionSpec,
)


def cfg() -> GatewayConfig:
    return GatewayConfig(
        versions=[
            VersionSpec(model="m", version="v1", latency=0.05),
            VersionSpec(model="m", version="v2", latency=0.05),
            VersionSpec(model="m", version="dead", latency=0.01,
                        fault="always_fail"),
        ],
        fallback_chains={},
        batch=BatchConfig(window=0.05, max_size=8),
        resources=ResourceConfig(
            global_concurrency=8, global_queue=64,
            per_model_concurrency=4, per_model_queue=32),
        retry=RetryConfig(max_retries=0),
        cache=CacheConfig(capacity=64, enabled=True),
    )


@pytest.mark.asyncio
async def test_concurrent_duplicate_id_single_inference(client_factory):
    async with client_factory(cfg()) as (client, app):
        payload = {"model": "m", "version": "v1",
                   "input": {"q": "same"}, "request_id": "dup-conc"}

        async def call():
            return await client.post("/v1/infer", json=payload)

        # 8 个完全相同的并发请求（同 request_id 同输入）
        results = await asyncio.gather(*[call() for _ in range(8)])
        bodies = [r.json() for r in results]
        # 全部成功且输出一致
        assert all(r.status_code == 200 for r in results)
        outputs = {b["output"] for b in bodies}
        assert len(outputs) == 1
        # 恰好一个主执行，其余都是去重命中
        dedup_flags = sorted(b["deduplicated"] for b in bodies)
        assert dedup_flags == [False] + [True] * 7

        st = app.state.gw.stats.snapshot()
        assert st["received"] == 1
        assert st["succeeded"] == 1
        assert st["dedup_hits"] == 7
        # 真实推理只发生一次（该输入只进入后端一次）
        assert st["per_version"]["m@v1"]["real_inferences"] == 1
        backend = app.state.gw.registry.get_backend("m", "v1")
        assert backend.call_count == 1
        print(f"\n[并发去重] request_id=dup-conc x8 -> 真实推理 1 次, "
              f"去重命中 7 次, 用量只计 1 次 reason=atomic_inflight_join")


@pytest.mark.asyncio
async def test_sequential_duplicate_id_after_completion(client_factory):
    async with client_factory(cfg()) as (client, app):
        body = {"model": "m", "version": "v1", "input": {"q": "x"},
                "request_id": "dup-seq"}
        r1 = await client.post("/v1/infer", json=body)
        r2 = await client.post("/v1/infer", json=body)
        assert r1.status_code == r2.status_code == 200
        assert r1.json()["output"] == r2.json()["output"]
        assert r2.json()["deduplicated"] is True
        st = app.state.gw.stats.snapshot()
        assert st["received"] == 1 and st["succeeded"] == 1
        print(f"\n[紧接重复] request_id=dup-seq 第二次提交不重复推理 "
              f"reason=bounded_completed_history")


@pytest.mark.asyncio
async def test_same_id_different_payload_rejected(client_factory):
    async with client_factory(cfg()) as (client, app):
        r1 = await client.post("/v1/infer", json={
            "model": "m", "version": "v1", "input": {"q": 1},
            "request_id": "dup-conflict"})
        r2 = await client.post("/v1/infer", json={
            "model": "m", "version": "v1", "input": {"q": 2},
            "request_id": "dup-conflict"})
        assert r1.status_code == 200
        assert r2.status_code == 409
        assert r2.json()["error"] == "duplicate_id_conflict"
        print(f"\n[冲突] 同 ID 不同负载 -> 409 reason=id_fingerprint_mismatch")


@pytest.mark.asyncio
async def test_same_id_different_target_rejected(client_factory):
    async with client_factory(cfg()) as (client, app):
        r1 = await client.post("/v1/infer", json={
            "model": "m", "version": "v1", "input": {"q": 1},
            "request_id": "target-conflict"})
        # 同 ID、同输入，但目标版本不同 -> 冲突
        r2 = await client.post("/v1/infer", json={
            "model": "m", "version": "v2", "input": {"q": 1},
            "request_id": "target-conflict"})
        assert r1.status_code == 200
        assert r2.status_code == 409
        assert r2.json()["error"] == "duplicate_id_conflict"
        print("\n[目标冲突] 同 ID 不同版本 -> 409 reason=fingerprint_has_target")


@pytest.mark.asyncio
async def test_failed_requests_not_counted_as_success(client_factory):
    async with client_factory(cfg()) as (client, app):
        results = await asyncio.gather(*[
            client.post("/v1/infer", json={
                "model": "m", "version": "dead",
                "input": {"i": i}, "request_id": f"fail-{i}"})
            for i in range(5)
        ])
        assert all(r.status_code == 503 for r in results)
        st = app.state.gw.stats.snapshot()
        # 守恒：5 接收 = 0 成功 + 5 失败
        assert st["received"] == 5
        assert st["succeeded"] == 0
        assert st["failed"] == 5
        print(f"\n[失败守恒] 5 失败请求: received=5 succeeded=0 failed=5 "
              f"reason=failures_excluded_from_success_usage")


@pytest.mark.asyncio
async def test_mixed_workload_conservation(client_factory):
    async with client_factory(cfg()) as (client, app):
        ok_ids = [f"ok-{i}" for i in range(4)]
        fail_ids = [f"fail-{i}" for i in range(3)]

        async def one(rid, ok):
            version = "v1" if ok else "dead"
            r = await client.post("/v1/infer", json={
                "model": "m", "version": version,
                "input": {"i": rid}, "request_id": rid})
            return r.status_code

        codes = await asyncio.gather(
            *[one(i, True) for i in ok_ids],
            *[one(i, False) for i in fail_ids],
        )
        st = app.state.gw.stats.snapshot()
        assert st["succeeded"] + st["failed"] == st["received"] == 7
        assert codes.count(200) == 4
        assert codes.count(503) == 3
        print(f"\n[混合守恒] received=7 = succeeded=4 + failed=3 "
              f"reason=every_request_settled_exactly_once")
