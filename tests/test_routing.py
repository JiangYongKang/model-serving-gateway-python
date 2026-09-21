"""路由与别名规则测试。"""

from __future__ import annotations

import pytest

from app.config import default_config


@pytest.mark.asyncio
async def test_version_and_alias_routing(client_factory):
    async with client_factory(default_config()) as (client, _app):
        # 具体版本号
        r = await client.post("/v1/infer", json={
            "model": "math", "version": "v2",
            "input": {"a": 1}, "request_id": "route-1"})
        assert r.status_code == 200
        assert r.json()["version"] == "v2"

        # 别名
        r = await client.post("/v1/infer", json={
            "model": "math", "version": "stable",
            "input": {"a": 1}, "request_id": "route-2"})
        assert r.json()["version"] == "v1"

        # 缺省版本 -> 该模型第一个版本 v1
        r = await client.post("/v1/infer", json={
            "model": "math", "input": {"a": 1}, "request_id": "route-3"})
        assert r.json()["version"] == "v1"


@pytest.mark.asyncio
async def test_rejections_are_distinguishable(client_factory):
    async with client_factory(default_config()) as (client, _app):
        cases = [
            ({"model": "ghost", "input": {}}, 404, "unknown_model"),
            ({"model": "math", "version": "v9", "input": {}},
             404, "unknown_version"),
            ({"model": "math", "version": "no-such-alias", "input": {}},
             404, "unknown_version"),
        ]
        for body, status, reason in cases:
            r = await client.post("/v1/infer", json=body)
            assert r.status_code == status
            assert r.json()["error"] == reason, r.text

        # 下线 v1 后：具体版本与指向它的别名都应返回 model_offline
        r = await client.post(
            "/admin/models/math/versions/v1/offline",
            params={"offline": True})
        assert r.status_code == 200
        r = await client.post("/v1/infer", json={
            "model": "math", "version": "v1", "input": {},
            "request_id": "off-1"})
        assert r.status_code == 410
        assert r.json()["error"] == "model_offline"
        r = await client.post("/v1/infer", json={
            "model": "math", "version": "stable", "input": {},
            "request_id": "off-2"})
        assert r.status_code == 410
        assert r.json()["error"] == "model_offline"
        # v2 仍可用
        r = await client.post("/v1/infer", json={
            "model": "math", "version": "v2", "input": {},
            "request_id": "off-3"})
        assert r.status_code == 200


@pytest.mark.asyncio
async def test_alias_switch_is_atomic_and_versions_never_mix(client_factory):
    """切换别名期间，单请求结果只来自一个版本。"""
    async with client_factory(default_config()) as (client, _app):
        async def call(rid):
            return await client.post("/v1/infer", json={
                "model": "math", "version": "canary",
                "input": {"k": rid}, "request_id": rid})

        r1 = await call("sw-1")
        assert r1.json()["version"] == "v2"

        r = await client.post(
            "/admin/models/math/aliases/canary", params={"version": "v1"})
        assert r.status_code == 200

        r2 = await call("sw-2")
        # 切换后必须整体落在 v1；输出前缀与版本字段一致
        assert r2.json()["version"] == "v1"
        assert r2.json()["output"].startswith("math/v1:")

        # 切换到不存在版本必须被拒绝，别名保持不变
        r = await client.post(
            "/admin/models/math/aliases/canary", params={"version": "v404"})
        assert r.status_code == 404
        r3 = await call("sw-3")
        assert r3.json()["version"] == "v1"
