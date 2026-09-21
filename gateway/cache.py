"""结果缓存：键含模型版本与规范化输入，容量有上限，LRU 确定性淘汰。"""
from __future__ import annotations

from gateway.config import CacheConfig


def normalize_prompt(prompt: str) -> str:
    """规范化输入（去首尾空白、折叠内部空白、统一大小写）。"""
    raise NotImplementedError


class ResultCache:
    """有界 LRU 缓存；键 = (model, version, normalized_prompt)。"""

    def __init__(self, config: CacheConfig) -> None:
        self._config = config

    def get(self, model: str, version: str, prompt: str) -> str | None:
        raise NotImplementedError

    def put(self, model: str, version: str, prompt: str, result: str) -> None:
        raise NotImplementedError
