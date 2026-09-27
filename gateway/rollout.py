"""灰度放量配套：分版本观测统计与自动止损（熔断）。

- VersionStats：每个 (model, version) 承接请求数、成功、失败、拒绝、
  降级次数与实际执行的推理次数；各版本合计与整体口径一致（守恒）。
- CircuitBreaker：按版本配置的失败比例阈值 + 观察窗口；
  窗口内失败比例达到阈值即触发止损（由网关注销该版本的分流资格）。
  只统计终态成功/失败，排队拒绝等不计入窗口。
"""
from __future__ import annotations

import threading
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field

from gateway.config import BreakerConfig


@dataclass
class VersionStats:
    """单版本观测口径。"""

    requests: int = 0        # 路由解析后归属本版本的请求数
    succeeded: int = 0
    failed: int = 0
    rejected: int = 0        # 解析后被拒绝（背压等）
    fallbacks: int = 0       # 归属本版本的请求中发生降级的数量
    real_inferences: int = 0  # 实际由本版本后端执行的推理次数（含作为降级目标）

    def snapshot(self) -> dict[str, int]:
        return dict(self.__dict__)


class VersionStatsRegistry:
    """分版本统计；键为 (model, version)。单事件循环 + 锁保护，计数守恒。"""

    def __init__(self) -> None:
        self._stats: dict[tuple[str, str], VersionStats] = {}
        self._lock = threading.Lock()

    def get(self, model: str, version: str) -> VersionStats:
        key = (model, version)
        with self._lock:
            stats = self._stats.get(key)
            if stats is None:
                stats = self._stats[key] = VersionStats()
            return stats

    def snapshot(self) -> dict[str, dict[str, int]]:
        with self._lock:
            items = list(self._stats.items())
        return {f"{m}@{v}": s.snapshot() for (m, v), s in sorted(items)}


class CircuitBreaker:
    """单版本自动止损：滑动窗口内失败比例达到阈值即触发。"""

    def __init__(self, model: str, version: str, config: BreakerConfig) -> None:
        self.model = model
        self.version = version
        self._cfg = config
        self._window: deque[bool] = deque(maxlen=config.window)  # True=失败
        self.tripped = False

    def record(self, failed: bool) -> bool:
        """记录一次终态结果；返回本次是否触发了止损。"""
        if self.tripped:
            return False
        self._window.append(failed)
        if len(self._window) >= self._cfg.window:
            ratio = sum(self._window) / len(self._window)
            if ratio >= self._cfg.failure_ratio:
                self.tripped = True
                return True
        return False


class BreakerRegistry:
    """管理所有版本的熔断器；触发时回调网关注销分流资格。"""

    def __init__(self, on_trip: Callable[[str, str], None]) -> None:
        self._breakers: dict[tuple[str, str], CircuitBreaker] = {}
        self._on_trip = on_trip
        self._lock = threading.Lock()

    def configure(self, model: str, version: str, config: BreakerConfig) -> None:
        with self._lock:
            self._breakers[(model, version)] = CircuitBreaker(model, version, config)

    def record(self, model: str, version: str, failed: bool) -> None:
        with self._lock:
            breaker = self._breakers.get((model, version))
        if breaker is None:
            return
        if breaker.record(failed):
            self._on_trip(model, version)

    def is_tripped(self, model: str, version: str) -> bool:
        with self._lock:
            breaker = self._breakers.get((model, version))
            return breaker.tripped if breaker is not None else False

    def reset(self, model: str, version: str) -> None:
        with self._lock:
            breaker = self._breakers.get((model, version))
            if breaker is not None:
                breaker.tripped = False
                breaker._window.clear()
