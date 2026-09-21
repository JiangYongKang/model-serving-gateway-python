"""确定性本地推理后端：模拟多个模型版本。

输出由 (model, version, prompt) 唯一决定，便于断言版本路由结果。
故障注入是确定性的：
- fail_prompts：对特定输入永久失败（单项失败 ItemFailure，不污染同批其他项）；
- transient_failures：接下来 N 次 infer_batch 整体失败（BackendError，用于重试/降级测试）。
"""
from __future__ import annotations

import asyncio
import hashlib

from gateway.config import VersionSpec


class BackendError(Exception):
    """后端整体故障（可重试/可降级）。"""


class ItemFailure(BackendError):
    """单项输入的确定性失败（不重试、不降级）。"""


class LocalBackend:
    """确定性本地后端。"""

    def __init__(self, model: str, spec: VersionSpec) -> None:
        self.model = model
        self.spec = spec
        self.transient_failures = 0  # 测试可设置：接下来 N 次调用整体失败
        self.call_count = 0          # 真实推理调用次数（用于断言重试/去重不计重复）

    async def infer_batch(self, prompts: list[str]) -> list[str | ItemFailure]:
        """对一批输入做确定性推理。

        返回与输入等长的列表；单项失败以 ItemFailure 占位。
        整体故障（transient）时抛 BackendError。
        """
        self.call_count += 1
        if self.transient_failures > 0:
            self.transient_failures -= 1
            raise BackendError(f"transient backend failure on {self.model}@{self.spec.version}")
        await asyncio.sleep(self.spec.latency_ms / 1000.0)
        results: list[str | ItemFailure] = []
        for p in prompts:
            if p in self.spec.fail_prompts:
                results.append(ItemFailure(f"deterministic item failure: {p!r}"))
            else:
                digest = hashlib.sha1(p.encode("utf-8")).hexdigest()[:8]
                results.append(f"{self.model}@{self.spec.version}#{digest}")
        return results
