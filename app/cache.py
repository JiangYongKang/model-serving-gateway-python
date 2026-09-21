"""有界确定性结果缓存。

缓存键 = (model, version, canonical_input)，天然包含版本：
别名切换后解析出的版本号变化，不会命中旧版本缓存。

淘汰策略：容量达上限时淘汰最早插入项（FIFO/插入序），
不做 LRU 触达更新，行为完全确定、可解释。
"""

from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any

_CACHE_MISS = object()


class ResultCache:
    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self._data: "OrderedDict[tuple[str, str, str], Any]" = OrderedDict()
        self._lock = threading.Lock()
        self.hits = 0

    def get(self, key: tuple[str, str, str]) -> Any | None:
        with self._lock:
            value = self._data.get(key, _CACHE_MISS)
            if value is _CACHE_MISS:
                return None
            # 命中不改变插入顺序（确定性 FIFO）。
            self.hits += 1
            return value

    def put(self, key: tuple[str, str, str], value: Any) -> None:
        with self._lock:
            if key in self._data:
                return  # 已存在不刷新位置
            self._data[key] = value
            while len(self._data) > self.capacity:
                self._data.popitem(last=False)  # 淘汰最旧

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()
