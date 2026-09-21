"""重试、降级与失败语义测试。"""

from __future__ import annotations

import pytest

from app.config import (
    BatchConfig,
    CacheConfig,
    GatewayConfig,
    ResourceConfig,
    RetryConfig,
    VersionSpec,
)


def cfg(max_retries: int) -> GatewayConfig:
    return GatewayConfig(
        versions=[
            VersionSpec(model="m", version="good", latency=0.0),
            VersionSpec(model="m", version="flaky", latency=0.0,
                        fault="fail_first_attempt_per_input"),
            VersionSpec(model="m", version="dead", latency=0.0,
                        fault="always_fail"),
            VersionSpec(model="m", version="ghost", latency=0.0,
                        fault="always_fail"),
        ],
        # ghost 配置一个根本不存在的降级目标
        fallback_chains={"m:dead": "good", "m:ghost": "v404"},
        batch=BatchConfig(window=0.02, max_size=8),
        resources=ResourceConfig(),
        retry=RetryConfig(max_retries=max_retries),
        cache=CacheConfig(capacity=64, enabled=True),
    )


@pytest.mark.asyncio
async def test_retry_succeeds_and_counts_once(client_factory):
    async with client_factory(cfg(max_retries=2)) as (client, app):
        r = await client.post("/v1/infer", json={
            "model": "m", "version": "flaky", "input": {"x": 1},
            "request_id": "retry-1"})
        assert r.status_code == 200
        body = r.json()
        assert body["attempts"] == 2  # 首次失败 + 一次重试成功
        assert body["fallback"] is None

        st = app.state.gw.stats.snapshot()
        assert st["received"] == 1
        assert st["succeeded"] == 1
        assert st["failed"] == 0
        assert st["retries"] == 1
        # 只有一次成功用量；真实推理 2 次（1 失败 + 1 成功）
        assert st["per_version"]["m@flaky"]["real_inferences"] == 2
        print(f"\n[重试] request_id=retry-1 attempts=2 "
              f"reason=first_attempt_failed_retry_ok; 成功用量只计 1 次")


@pytest.mark.asyncio
async def test_fallback_success_reflects_target(client_factory):
    async with client_factory(cfg(max_retries=0)) as (client, app):
        r = await client.post("/v1/infer", json={
            "model": "m", "version": "dead", "input": {"x": 1},
            "request_id": "fb-1"})
        assert r.status_code == 200
        body = r.json()
        assert body["version"] == "good"
        assert body["fallback"] == "good"
        assert body["output"].startswith("m/good:")

        st = app.state.gw.stats.snapshot()
        assert st["fallbacks"] == 1
        assert st["succeeded"] == 1 and st["failed"] == 0
        print(f"\n[降级成功] request_id=fb-1 dead -> good "
              f"reason=primary_unavailable_target_ready")


@pytest.mark.asyncio
async def test_fallback_to_missing_version_fails_explicitly(client_factory):
    async with client_factory(cfg(max_retries=0)) as (client, app):
        r = await client.post("/v1/infer", json={
            "model": "m", "version": "ghost", "input": {"x": 1},
            "request_id": "fb-2"})
        assert r.status_code == 503
        assert r.json()["error"] == "fallback_unavailable"

        st = app.state.gw.stats.snapshot()
        # 明确失败：不产生伪结果，失败计入失败用量
        assert st["failed"] == 1 and st["succeeded"] == 0
        print(f"\n[降级不可用] request_id=fb-2 reason=target_not_registered "
              f"-> 503 fallback_unavailable，无伪造结果")


@pytest.mark.asyncio
async def test_fallback_target_offline_is_unavailable(client_factory):
    async with client_factory(cfg(max_retries=0)) as (client, app):
        # 把降级目标 good 下线，再请求 dead
        await client.post(
            "/admin/models/m/versions/good/offline", params={"offline": True})
        r = await client.post("/v1/infer", json={
            "model": "m", "version": "dead", "input": {"x": 1},
            "request_id": "fb-3"})
        assert r.status_code == 503
        assert r.json()["error"] == "fallback_unavailable"
        print(f"\n[降级目标下线] request_id=fb-3 reason=target_offline "
              f"-> 明确失败")


@pytest.mark.asyncio
async def test_retry_exhausted_without_fallback_fails(client_factory):
    # dead 在这里临时去掉降级链
    c = cfg(max_retries=1)
    object.__setattr__(c, "fallback_chains", {})
    async with client_factory(c) as (client, app):
        r = await client.post("/v1/infer", json={
            "model": "m", "version": "dead", "input": {"x": 1},
            "request_id": "ex-1"})
        assert r.status_code == 503
        assert r.json()["error"] == "fallback_unavailable"
        st = app.state.gw.stats.snapshot()
        assert st["failed"] == 1 and st["retries"] == 1
