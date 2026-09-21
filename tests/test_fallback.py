"""重试与降级：重试不重复计用量；降级目标可用性真实反映；降级不可用明确失败。"""
from __future__ import annotations

from gateway.config import ModelSpec, VersionSpec
from gateway.errors import GatewayError, Reason
from gateway.gateway import Gateway
from tests.conftest import log, make_config


async def test_retry_on_transient_failure_counts_once():
    gw = Gateway(make_config())
    await gw.start()
    try:
        backend = gw.backends[("m", "v1")]
        backend.call_count = 0  # 扣除预热调用
        backend.transient_failures = 1  # 第一次整体失败，重试成功
        result = await gw.infer("m", "retry-me", request_id="r-retry")
        s = gw.stats.snapshot()
        log.info("req=r-retry result=%s backend_calls=%d stats=%s",
                 result, backend.call_count, s)
        assert result.startswith("m@v1#")
        assert backend.call_count == 2          # 真实重试发生
        assert s["real_inferences"] == 1        # 但用量只计一次
        assert s["succeeded"] == 1 and s["fallbacks"] == 0
    finally:
        await gw.shutdown()


async def test_fallback_to_other_version():
    gw = Gateway(make_config())
    await gw.start()
    try:
        backend = gw.backends[("m", "v1")]
        backend.transient_failures = 5  # 重试耗尽 -> 降级到 v2
        result = await gw.infer("m", "fallback-me", request_id="r-fb")
        s = gw.stats.snapshot()
        log.info("req=r-fb result=%s stats=%s", result, s)
        assert result.startswith("m@v2#")       # 结果来自降级版本
        assert s["fallbacks"] == 1
        assert s["real_inferences"] == 1        # 降级不重复计用量
        assert s["succeeded"] == 1
    finally:
        await gw.shutdown()


async def test_fallback_unavailable_fails_explicitly():
    """降级目标也已下线时，必须明确失败而非返回伪造结果。"""
    cfg = make_config()
    gw = Gateway(cfg)
    await gw.start()
    try:
        gw.registry.retire_version("m", "v2")   # 下线唯一降级目标
        backend = gw.backends[("m", "v1")]
        backend.transient_failures = 5
        try:
            await gw.infer("m", "no-fallback", request_id="r-nofb")
            raise AssertionError("should have failed")
        except GatewayError as exc:
            log.info("req=r-nofb reason=%s", exc.reason)
            assert exc.reason is Reason.FALLBACK_UNAVAILABLE
        s = gw.stats.snapshot()
        log.info("stats=%s", s)
        assert s["failed"] == 1 and s["succeeded"] == 0
    finally:
        await gw.shutdown()


async def test_backend_error_without_fallback():
    """无降级链的模型后端故障 -> BACKEND_ERROR。"""
    cfg = make_config()
    cfg.models["m"] = ModelSpec(  # 去掉降级链
        name="m",
        versions={"v1": VersionSpec(version="v1", latency_ms=5.0)},
        aliases={"latest": "v1"},
    )
    gw = Gateway(cfg)
    await gw.start()
    try:
        gw.backends[("m", "v1")].transient_failures = 5
        try:
            await gw.infer("m", "boom", request_id="r-boom")
            raise AssertionError("should have failed")
        except GatewayError as exc:
            log.info("req=r-boom reason=%s", exc.reason)
            assert exc.reason is Reason.BACKEND_ERROR
    finally:
        await gw.shutdown()
