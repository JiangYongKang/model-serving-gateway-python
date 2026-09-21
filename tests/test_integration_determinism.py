"""端到端确定性：默认配置下多次运行结果一致、跨特性行为可预期。"""

from __future__ import annotations

import asyncio

import pytest

from app.config import default_config


@pytest.mark.asyncio
async def test_deterministic_outputs_across_runs(client_factory):
    async def one_run():
        out = {}
        async with client_factory(default_config()) as (client, _app):
            for rid, ver, val in [
                ("i-1", "v1", {"a": 1}),
                ("i-2", "stable", {"a": 1}),   # stable == v1，同输入
                ("i-3", "canary", {"a": 1}),   # v2
                ("i-4", "v2", {"z": [1, 2, {"k": "v"}]}),
            ]:
                r = await client.post("/v1/infer", json={
                    "model": "math", "version": ver,
                    "input": val, "request_id": rid})
                assert r.status_code == 200
                out[rid] = (r.json()["version"], r.json()["output"])
        return out

    run1, run2 = await asyncio.gather(one_run(), one_run())
    assert run1 == run2
    # 具体版本与别名解析到同一版本时，输出一致
    assert run1["i-1"] == run1["i-2"]
    assert run1["i-3"][0] == "v2"
    print(f"\n[确定性] 两次独立运行结果完全一致: {run1}")


@pytest.mark.asyncio
async def test_full_scenario_stats_conservation(client_factory):
    async with client_factory(default_config()) as (client, app):
        async def infer(rid, model="math", version=None, body=None):
            return await client.post("/v1/infer", json={
                "model": model, "version": version,
                "input": body or {"rid": rid}, "request_id": rid})

        # 并发：4 个 math 请求（应合批）
        batch = await asyncio.gather(*[
            infer(f"p-{i}", version="v2") for i in range(4)
        ])
        batch_ids = {r.json()["batch_id"] for r in batch}
        assert len(batch_ids) == 1

        # 版本切换：stable v1 -> v2，切换后同输入走新版本
        before = await infer("sw-before", version="stable", body={"k": 9})
        assert before.json()["version"] == "v1"
        await client.post("/admin/models/math/aliases/stable",
                          params={"version": "v2"})
        after = await infer("sw-after", version="stable", body={"k": 9})
        assert after.json()["version"] == "v2"
        assert after.json()["cached"] is False  # 版本变了，不命中旧缓存

        # 降级：flaky v1 -> v2
        fb = await infer("fb-x", model="flaky", version="v1")
        assert fb.json()["fallback"] == "v2"

        # 降级不可用：doom v1 -> v2(不存在)
        dm = await infer("dm-x", model="doom", version="v1")
        assert dm.status_code == 503

        st = app.state.gw.stats.snapshot()
        # 守恒：成功 + 失败 == 接收（去重/缓存命中不改这一恒等式）
        assert st["succeeded"] + st["failed"] == st["received"]
        assert st["failed"] == 1 and st["fallbacks"] == 1
        print(f"\n[端到端] received={st['received']} "
              f"succeeded={st['succeeded']} failed={st['failed']} "
              f"real_inf={st['real_inferences']} retries={st['retries']} "
              f"fallbacks={st['fallbacks']} cache={st['cache_hits']} "
              f"-> 守恒且可解释")
