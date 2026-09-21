"""用量统计。

守恒约束：
- 每个被接收的请求最终恰好计入 succeeded 或 failed 之一（缓存/去重命中
  也算成功，但不计入真实推理）；
- real_inferences 统计后端真实处理的输入条数（含失败的尝试）；
- 失败请求不计入成功用量；重试不重复计入 succeeded。
"""

from __future__ import annotations

import threading
from typing import Any

_FIELDS = (
    "received", "succeeded", "failed", "cache_hits", "dedup_hits",
    "retries", "fallbacks", "real_inferences",
)


class Stats:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.received = 0
        self.succeeded = 0
        self.failed = 0
        self.cache_hits = 0
        self.dedup_hits = 0
        self.retries = 0
        self.fallbacks = 0
        self.real_inferences = 0
        self.per_version: dict[str, dict[str, int]] = {}

    def _bucket(self, key: str) -> dict[str, int]:
        bucket = self.per_version.get(key)
        if bucket is None:
            bucket = {f: 0 for f in _FIELDS}
            self.per_version[key] = bucket
        return bucket

    def record_received(self, key: str) -> None:
        with self._lock:
            self.received += 1
            self._bucket(key)["received"] += 1

    def record_success(
        self, key: str, *, cached: bool = False, dedup: bool = False
    ) -> None:
        with self._lock:
            self.succeeded += 1
            b = self._bucket(key)
            b["succeeded"] += 1
            if cached:
                self.cache_hits += 1
                b["cache_hits"] += 1
            if dedup:
                self.dedup_hits += 1
                b["dedup_hits"] += 1

    def record_failure(self, key: str) -> None:
        with self._lock:
            self.failed += 1
            self._bucket(key)["failed"] += 1

    def record_dedup_hit(self, key: str) -> None:
        """去重命中：只观测，不增加用量（received/succeeded 不变）。"""
        with self._lock:
            self.dedup_hits += 1
            self._bucket(key)["dedup_hits"] += 1

    def record_retry(self, key: str) -> None:
        with self._lock:
            self.retries += 1
            self._bucket(key)["retries"] += 1

    def record_fallback(self, key: str) -> None:
        with self._lock:
            self.fallbacks += 1
            self._bucket(key)["fallbacks"] += 1

    def record_real(self, key: tuple[str, str], n: int = 1) -> None:
        kv = f"{key[0]}@{key[1]}"
        with self._lock:
            self.real_inferences += n
            self._bucket(kv)["real_inferences"] += n

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "received": self.received,
                "succeeded": self.succeeded,
                "failed": self.failed,
                "cache_hits": self.cache_hits,
                "dedup_hits": self.dedup_hits,
                "retries": self.retries,
                "fallbacks": self.fallbacks,
                "real_inferences": self.real_inferences,
                "per_version": {
                    k: dict(v) for k, v in sorted(self.per_version.items())
                },
            }
