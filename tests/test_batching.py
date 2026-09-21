"""动态合批：并发请求合并成批；不同版本不混批；批内单项失败不污染其他请求。"""
from __future__ import annotations

import asyncio

import pytest

from gateway.errors import GatewayError, Reason
from gateway.gateway import Gateway
from tests.conftest import log, make_config


async def test_concurrent_requests_are_batched():
    gw = Gateway(make_config())
    await gw.start()
    try:
        n = 8  # 等于 max_batch_size
        results = await asyncio.gather(
            *(gw.infer("m", f"batch-{i}", request_id=f"rb-{i}") for i in range(n))
        )
        calls = gw.backends[("m", "v1")].call_count
        log.info("batch test: n=%d backend_calls=%d results=%s", n, calls, results)
        assert len(set(results)) == n          # 每条结果对应各自输入
        assert all(r.startswith("m@v1#") for r in results)
        assert 1 <= calls < n                   # 合批生效：调用次数小于请求数
    finally:
        await gw.shutdown()


async def test_different_versions_never_share_batch():
    gw = Gateway(make_config())
    await gw.start()
    try:
        results = await asyncio.gather(
            *(gw.infer("m", f"mix-{i}", version_or_alias="v1" if i % 2 == 0 else "v2",
                       request_id=f"rm-{i}") for i in range(8))
        )
        v1 = [r for i, r in enumerate(results) if i % 2 == 0]
        v2 = [r for i, r in enumerate(results) if i % 2 != 0]
        log.info("mixed versions: v1=%s v2=%s", v1, v2)
        assert all(r.startswith("m@v1#") for r in v1)
        assert all(r.startswith("m@v2#") for r in v2)
    finally:
        await gw.shutdown()


async def test_item_failure_does_not_pollute_batch_mates():
    gw = Gateway(make_config(fail_prompts=frozenset({"bad-input"})))
    await gw.start()
    try:
        async def good(i: int) -> str:
            return await gw.infer("m", f"good-{i}", request_id=f"rg-{i}")

        async def bad() -> str:
            return await gw.infer("m", "bad-input", request_id="r-bad")

        gathered = await asyncio.gather(good(0), bad(), good(1), return_exceptions=True)
        log.info("item failure batch: results=%s", gathered)
        assert gathered[0].startswith("m@v1#")
        assert gathered[2].startswith("m@v1#")
        assert isinstance(gathered[1], GatewayError)
        assert gathered[1].reason is Reason.BACKEND_ERROR
        # 统计守恒：2 成功 + 1 失败
        s = gw.stats.snapshot()
        log.info("stats=%s", s)
        assert s["succeeded"] == 2 and s["failed"] == 1
        assert s["accepted"] == s["succeeded"] + s["failed"] + s["rejected"]
    finally:
        await gw.shutdown()
