"""生命周期：未就绪拒绝；就绪反映后端真实可用性；停机时在途请求完成、新请求明确拒绝。"""
from __future__ import annotations

import asyncio

import pytest

from gateway.errors import GatewayError, Reason
from gateway.gateway import Gateway
from tests.conftest import log, make_config


async def test_not_ready_before_start():
    gw = Gateway(make_config())
    assert not gw.ready
    with pytest.raises(GatewayError) as ei:
        await gw.infer("m", "x", request_id="rl-notready")
    log.info("req=rl-notready reason=%s", ei.value.reason)
    assert ei.value.reason is Reason.NOT_READY


async def test_ready_reflects_backend_availability():
    gw = Gateway(make_config())
    await gw.start()
    try:
        assert gw.ready
        gw.registry.retire_version("m", "v1")
        gw.registry.retire_version("m", "v2")
        # 版本全部下线后，请求被明确拒绝（VERSION_RETIRED），而非返回过期结果
        with pytest.raises(GatewayError) as ei:
            await gw.infer("m", "x", request_id="rl-retired")
        log.info("req=rl-retired reason=%s", ei.value.reason)
        assert ei.value.reason is Reason.VERSION_RETIRED
    finally:
        await gw.shutdown()


async def test_graceful_shutdown_completes_inflight_and_rejects_new():
    gw = Gateway(make_config())
    await gw.start()
    inflight = asyncio.create_task(gw.infer("slow", "long", request_id="rl-inflight"))
    await asyncio.sleep(0.05)  # 确保请求已进入执行
    shutdown = asyncio.create_task(gw.shutdown())
    await asyncio.sleep(0.02)
    # 停机期间新请求被明确拒绝
    with pytest.raises(GatewayError) as ei:
        await gw.infer("m", "x", request_id="rl-during-shutdown")
    log.info("req=rl-during-shutdown reason=%s", ei.value.reason)
    assert ei.value.reason is Reason.SHUTTING_DOWN
    # 在途请求正常完成，停机随后结束
    result = await inflight
    await shutdown
    log.info("req=rl-inflight result=%s", result)
    assert result.startswith("slow@v1#")
    assert not gw.ready
