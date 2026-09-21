"""结果缓存语义测试。"""

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


def cfg(capacity: int = 4) -> GatewayConfig:
    return GatewayConfig(
        versions=[
            VersionSpec(model="m", version="v1", latency=0.0),
            VersionSpec(model="m", version="v2", latency=0.0),
        ],
        batch=BatchConfig(window=0.02, max_size=8),
        resources=ResourceConfig(),
        retry=RetryConfig(max_retries=0),
        cache=CacheConfig(capacity=capacity),
    )


@pytest.mark.asyncio
async def test_canonical_input_shares_cache(client_factory):
    async with client_factory(cfg()) as (client, app):
        # 键顺序不同但 JSON 规范化后等价
        r1 = await client.post("/v1/infer", json={
            "model": "m", "version": "v1",
            "input": {"a": 1, "b": 2}, "request_id": "c-1"})
        r2 = await client.post("/v1/infer", json={
            "model": "m", "version": "v1",
            "input": {"b": 2, "a": 1}, "request_id": "c-2"})
        assert r1.json()["output"] == r2.json()["output"]
        assert r1.json()["cached"] is False
        assert r2.json()["cached"] is True
        print(f"\n[缓存规范化] request_id=c-2 cached=true "
              f"reason=canonical_json_key_equal")


@pytest.mark.asyncio
async def test_version_change_misses_old_cache(client_factory):
    async with client_factory(cfg()) as (client, app):
        r1 = await client.post("/v1/infer", json={
            "model": "m", "version": "v1",
            "input": {"x": 1}, "request_id": "v-1"})
        r2 = await client.post("/v1/infer", json={
            "model": "m", "version": "v2",
            "input": {"x": 1}, "request_id": "v-2"})
        assert r1.json()["cached"] is False
        assert r2.json()["cached"] is False
        assert r1.json()["output"] != r2.json()["output"]
        print(f"\n[版本隔离] 同输入 v1/v2 不共享缓存 "
              f"reason=cache_key_includes_version")


@pytest.mark.asyncio
async def test_alias_switch_invalidates(client_factory):
    """别名从 v1 切到 v2 后，同输入不得命中 v1 的旧缓存。"""
    async with client_factory(cfg()) as (client, app):
        # 先建立别名
        await client.post("/admin/models/m/aliases/ptr", params={"version": "v1"})
        r1 = await client.post("/v1/infer", json={
            "model": "m", "version": "ptr",
            "input": {"x": 42}, "request_id": "a-1"})
        assert r1.json()["version"] == "v1" and r1.json()["cached"] is False

        # 切别名到 v2
        await client.post("/admin/models/m/aliases/ptr", params={"version": "v2"})
        r2 = await client.post("/v1/infer", json={
            "model": "m", "version": "ptr",
            "input": {"x": 42}, "request_id": "a-2"})
        assert r2.json()["version"] == "v2"
        assert r2.json()["cached"] is False
        assert r2.json()["output"].startswith("m/v2:")
        print(f"\n[别名切换] ptr: v1 -> v2 后不命中旧缓存 "
              f"reason=resolved_version_changed")


@pytest.mark.asyncio
async def test_fifo_eviction_is_deterministic(client_factory):
    async with client_factory(cfg(capacity=2)) as (client, app):
        cache = app.state.gw.cache

        async def fresh(rid, version, val):
            r = await client.post("/v1/infer", json={
                "model": "m", "version": version,
                "input": {"k": val}, "request_id": rid})
            return r

        await fresh("e-1", "v1", 1)  # 插入键 A
        await fresh("e-2", "v1", 2)  # 插入键 B，缓存满
        # 命中 A 不刷新 FIFO 位置
        r = await client.post("/v1/infer", json={
            "model": "m", "version": "v1",
            "input": {"k": 1}, "request_id": "e-3"})
        assert r.json()["cached"] is True
        await fresh("e-4", "v2", 3)  # 插入键 C(v2) -> 淘汰最旧 A(v1)
        assert len(cache) == 2
        # A 已被淘汰、B 仍保留（直接检查缓存内容，避免未命中又写回）
        keys = set(cache._data.keys())
        assert ("m", "v1", '{"k":1}') not in keys
        assert ("m", "v1", '{"k":2}') in keys
        assert ("m", "v2", '{"k":3}') in keys
        # B 仍可命中
        r = await client.post("/v1/infer", json={
            "model": "m", "version": "v1",
            "input": {"k": 2}, "request_id": "e-6-unique"})
        assert r.json()["cached"] is True
        print(f"\n[FIFO淘汰] capacity=2: 命中不刷新，插入C淘汰最旧A "
              f"reason=insertion_order_eviction")
