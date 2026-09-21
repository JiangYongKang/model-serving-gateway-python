"""缓存：键含版本与规范化输入；版本/别名变更后旧缓存不命中；容量上限与确定性淘汰。"""
from __future__ import annotations

from gateway.gateway import Gateway
from tests.conftest import log, make_config


async def test_cache_hit_and_normalization():
    gw = Gateway(make_config())
    await gw.start()
    try:
        backend = gw.backends[("m", "v1")]
        backend.call_count = 0
        r1 = await gw.infer("m", "Hello   World", request_id="rc-1")
        r2 = await gw.infer("m", "  hello world ", request_id="rc-2")  # 规范化后相同
        s = gw.stats.snapshot()
        log.info("req=rc-1 result=%s; req=rc-2 result=%s calls=%d stats=%s",
                 r1, r2, backend.call_count, s)
        assert r1 == r2
        assert backend.call_count == 1   # 第二次命中缓存，未真实推理
        assert s["cache_hits"] == 1
    finally:
        await gw.shutdown()


async def test_version_change_invalidates_cache():
    gw = Gateway(make_config())
    await gw.start()
    try:
        r1 = await gw.infer("m", "cached", request_id="rc-old")   # latest -> v1，写入 v1 缓存
        gw.registry.set_alias("m", "latest", "v2")
        r2 = await gw.infer("m", "cached", request_id="rc-new")   # 同输入但版本不同
        s = gw.stats.snapshot()
        log.info("req=rc-old result=%s; req=rc-new result=%s stats=%s", r1, r2, s)
        assert r1.startswith("m@v1#")
        assert r2.startswith("m@v2#")    # 旧缓存未被命中
        assert s["cache_hits"] == 0
    finally:
        await gw.shutdown()


async def test_cache_capacity_and_deterministic_eviction():
    gw = Gateway(make_config())  # capacity=4
    await gw.start()
    try:
        for i in range(4):
            await gw.infer("m", f"item-{i}", request_id=f"rcap-{i}")
        # 触碰 item-0 使其成为最近使用
        await gw.infer("m", "item-0", request_id="rcap-touch")
        # 写入第 5 个，应淘汰最久未用的 item-1 而非 item-0
        await gw.infer("m", "item-4", request_id="rcap-4")
        backend = gw.backends[("m", "v1")]
        backend.call_count = 0
        r0 = await gw.infer("m", "item-0", request_id="rcap-hit0")
        await gw.infer("m", "item-1", request_id="rcap-miss1")
        calls = backend.call_count
        log.info("eviction: item-0 result=%s extra_calls=%d", r0, calls)
        assert calls == 1  # item-0 命中（0 次真实调用），item-1 被淘汰（1 次真实调用）
    finally:
        await gw.shutdown()
