"""启动预热与优雅停机测试。"""

from __future__ import annotations

import asyncio

import httpx
import pytest

from app.app import create_app
from app.config import (
    BatchConfig,
    GatewayConfig,
    ResourceConfig,
    RetryConfig,
    VersionSpec,
)
from app.registry import Registry


def cfg() -> GatewayConfig:
    return GatewayConfig(
        versions=[
            VersionSpec(model="m", version="v1", latency=0.01, warmup=0.05),
            VersionSpec(model="m", version="v2", latency=0.01, warmup=0.05),
        ],
        batch=BatchConfig(window=0.02, max_size=8),
        resources=ResourceConfig(),
        retry=RetryConfig(max_retries=0),
        shutdown_timeout=2.0,
    )


@pytest.mark.asyncio
async def test_readiness_reflects_backend_warmup():
    """预热完成前 not ready；完成后 ready 且反映每个后端。"""
    app = create_app(cfg())
    transport = httpx.ASGITransport(app=app)
    # 未进入生命周期：app.state.gw 尚不存在，跳过 HTTP，直接验证后端初始未就绪
    registry = Registry(cfg())
    assert registry.all_ready() is False
    await registry.warmup_all()
    assert registry.all_ready() is True
    assert all(v is True for v in registry.ready_snapshot().values())

    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://gw") as client:
            r = await client.get("/health/ready")
            body = r.json()
            assert body["ready"] is True
            assert body["shutting_down"] is False
            assert body["backends"] == {"m@v1": True, "m@v2": True}
    print("\n[预热] ready 严格反映全部后端完成预热 reason=warmup_all_done")


@pytest.mark.asyncio
async def test_graceful_shutdown_completes_inflight():
    """停机时在途请求应完成；停机后新请求被明确拒绝。"""
    app = create_app(cfg())
    transport = httpx.ASGITransport(app=app)

    async with app.router.lifespan_context(app) as _:
        client = httpx.AsyncClient(transport=transport, base_url="http://gw")
        # 发起一个在途请求（后端 0.01s + 批次窗口 0.02s）
        inflight = asyncio.create_task(client.post("/v1/infer", json={
            "model": "m", "version": "v1",
            "input": {"x": 1}, "request_id": "sd-1"}))
        await asyncio.sleep(0.01)  # 确保请求已在途

    # 生命周期退出（优雅停机）期间/之后，在途请求应拿到 200
    r = await inflight
    assert r.status_code == 200, r.text
    await client.aclose()
    print("[优雅停机] 在途请求 sd-1 在停机过程中完成 200 "
          "reason=drain_before_stop")


@pytest.mark.asyncio
async def test_new_request_rejected_during_shutdown():
    """标记停机后，新请求返回 shutting_down，而不是挂起。"""
    app = create_app(cfg())
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://gw") as client:
            app.state.gw.shutting_down = True
            r = await client.post("/v1/infer", json={
                "model": "m", "version": "v1",
                "input": {"x": 1}, "request_id": "sd-2"})
            assert r.status_code == 503
            assert r.json()["error"] == "shutting_down"
            hr = await client.get("/health/ready")
            assert hr.json()["ready"] is False
    print("[停机拒收] 新请求 -> 503 shutting_down, ready=false "
          "reason=shutdown_flag_checked_at_entry")
