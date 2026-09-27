"""结果缓存：键含模型版本与规范化输入，容量有上限，LRU 确定性淘汰。

键 = (model, version, normalized_prompt)，因此版本或别名变更后
旧缓存自然不会被命中（别名解析在缓存查询之前完成）。
"""
from __future__ import annotations

import threading
from collections import OrderedDict

from gateway.config import CacheConfig


def normalize_prompt(prompt: str) -> str:
    """规范化输入：去首尾空白、折叠内部空白、统一小写。"""
    return " ".join(prompt.split()).lower()


class ResultCache:
    def __init__(self, config: CacheConfig) -> None:
        self._config = config
        self._store: OrderedDict[tuple[str, str, str], str] = OrderedDict()
        self._lock = threading.Lock()

    def _key(self, model: str, version: str, prompt: str) -> tuple[str, str, str]:
        return (model, version, normalize_prompt(prompt))

    def get(self, model: str, version: str, prompt: str) -> str | None:
        if not self._config.enabled:
            return None
        key = self._key(model, version, prompt)
        with self._lock:
            if key not in self._store:
                return None
            self._store.move_to_end(key)  # LRU：命中即提到最新
            return self._store[key]

    def put(self, model: str, version: str, prompt: str, result: str) -> None:
        if not self._config.enabled:
            return
        key = self._key(model, version, prompt)
        with self._lock:
            self._store[key] = result
            self._store.move_to_end(key)
            while len(self._store) > self._config.capacity:
                self._store.popitem(last=False)  # 确定性淘汰最久未用项

    def invalidate(self, model: str, version: str) -> int:
        """使某版本的全部缓存项失效（回退/止损时调用），返回清除条数。"""
        with self._lock:
            doomed = [k for k in self._store if k[0] == model and k[1] == version]
            for k in doomed:
                del self._store[k]
        return len(doomed)

    def __len__(self) -> int:
        with self._lock:
            return len(self._store)
