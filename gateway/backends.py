"""确定性本地推理后端：模拟多个模型版本。"""
from __future__ import annotations

from gateway.config import VersionSpec


class BackendError(Exception):
    """后端模拟故障。"""


class LocalBackend:
    """确定性本地后端：输出由 (模型, 版本, 规范化输入) 唯一决定。"""

    def __init__(self, model: str, spec: VersionSpec) -> None:
        self.model = model
        self.spec = spec

    async def infer_batch(self, prompts: list[str]) -> list[str]:
        """对一批输入做确定性推理；失败时抛 BackendError。"""
        raise NotImplementedError
