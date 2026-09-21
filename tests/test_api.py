"""HTTP 入口：推理、就绪、统计、别名切换与错误原因码。"""
from __future__ import annotations

import httpx
import pytest

from gateway.app import create_app
from tests.conftest import log, make_config


@pytest.fixture
async def client():
    app = create_app(make_config())
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            yield c


async def test_infer_and_stats(client):
    r = await client.post("/infer", json={"model": "m", "prompt": "hi", "request_id": "ra-1"})
    assert r.status_code == 200
    body = r.json()
    log.info("req=ra-1 http result=%s", body)
    assert body["result"].startswith("m@v1#")
    s = (await client.get("/stats")).json()
    log.info("stats=%s", s)
    assert s["succeeded"] == 1
    assert s["accepted"] == s["succeeded"] + s["failed"] + s["rejected"]


async def test_error_reasons_distinguishable(client):
    cases = [
        ({"model": "nope", "prompt": "x"}, 404, "unknown_model"),
        ({"model": "m", "prompt": "x", "version": "v9"}, 404, "unknown_version"),
        ({"model": "m", "prompt": "x", "version": "v0"}, 410, "version_retired"),
    ]
    for payload, status, reason in cases:
        r = await client.post("/infer", json=payload)
        detail = r.json()["detail"]
        log.info("http payload=%s status=%d reason=%s", payload, r.status_code, detail["reason"])
        assert r.status_code == status
        assert detail["reason"] == reason


async def test_alias_switch_over_http(client):
    r1 = await client.post("/infer", json={"model": "m", "prompt": "sw", "request_id": "ra-sw1"})
    assert r1.json()["result"].startswith("m@v1#")
    r = await client.post("/admin/alias", json={"model": "m", "alias": "latest", "version": "v2"})
    assert r.status_code == 200
    r2 = await client.post("/infer", json={"model": "m", "prompt": "sw", "request_id": "ra-sw2"})
    log.info("alias switch: before=%s after=%s", r1.json()["result"], r2.json()["result"])
    assert r2.json()["result"].startswith("m@v2#")


async def test_ready_endpoint(client):
    r = await client.get("/ready")
    assert r.status_code == 200 and r.json()["ready"] is True
