"""路由与别名：未知模型/未知版本/已下线版本原因可区分；别名切换不跨版本混合。"""
from __future__ import annotations

import asyncio

import pytest

from gateway.errors import GatewayError, Reason
from tests.conftest import log


async def test_unknown_model(gw):
    with pytest.raises(GatewayError) as ei:
        await gw.infer("nope", "hello", request_id="r-unknown-model")
    log.info("req=r-unknown-model reason=%s", ei.value.reason)
    assert ei.value.reason is Reason.UNKNOWN_MODEL


async def test_unknown_version(gw):
    with pytest.raises(GatewayError) as ei:
        await gw.infer("m", "hello", version_or_alias="v9", request_id="r-unknown-ver")
    log.info("req=r-unknown-ver reason=%s", ei.value.reason)
    assert ei.value.reason is Reason.UNKNOWN_VERSION


async def test_retired_version(gw):
    with pytest.raises(GatewayError) as ei:
        await gw.infer("m", "hello", version_or_alias="v0", request_id="r-retired")
    log.info("req=r-retired reason=%s", ei.value.reason)
    assert ei.value.reason is Reason.VERSION_RETIRED


async def test_alias_and_explicit_version(gw):
    r_alias = await gw.infer("m", "hello", request_id="r-alias")          # latest -> v1
    r_v2 = await gw.infer("m", "hello", version_or_alias="v2", request_id="r-v2")
    log.info("req=r-alias result=%s; req=r-v2 result=%s", r_alias, r_v2)
    assert r_alias.startswith("m@v1#")
    assert r_v2.startswith("m@v2#")
    assert r_alias != r_v2


async def test_alias_switch_no_cross_version_mixing(gw):
    """别名切换前后，单请求结果只来自一个版本。"""
    before = await gw.infer("m", "world", request_id="r-before")
    gw.registry.set_alias("m", "latest", "v2")
    # 并发打一批请求，结果必须全部来自 v2（切换后），且每个结果单一版本
    results = await asyncio.gather(
        *(gw.infer("m", f"world-{i}", request_id=f"r-after-{i}") for i in range(10))
    )
    log.info("req=r-before result=%s; after-switch results=%s", before, set(results))
    assert before.startswith("m@v1#")
    assert all(r.startswith("m@v2#") for r in results)
