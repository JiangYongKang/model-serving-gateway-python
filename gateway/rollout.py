"""灰度放量观测与自动止损。

- VersionStats：分版本计数（承接/成功/失败/拒绝/降级/真实推理），
  各版本合计与整体口径一致（守恒由 Gateway 在同一判定点的记录保证）。
- RolloutMonitor：按 VersionSpec.breaker 配置维护每版本最近 N 个请求的
  滑动窗口，失败比例达阈值即回调 halt(model, version) 停止该版本放量；
  止损只影响新请求的分流，在途请求正常收尾并继续计数。
"""
from __future__ import annotations

import threading
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field

from gateway.config import GatewayConfig


@dataclass
class VersionStats:
    """单版本计数器。"""

    requests: int = 0         # 路由到本版本的请求数
    succeeded: int = 0
    failed: int = 0           # 后端/降级失败（与整体 failed 同口径）
    rejected: int = 0         # 路由之后的拒绝（背压/停机等）
    fallbacks: int = 0        # 从本版本降级出去的次数
    real_inferences: int = 0  # 归属本版本的真实推理（降级时归属实际产出结果的版本）

    def snapshot(self) -> dict[str, int]:
        return dict(self.__dict__)


class RolloutMonitor:
    """分版本统计 + 自动止损。线程安全；记录本身是 O(1)。"""

    def __init__(
        self,
        config: GatewayConfig,
        halt: Callable[[str, str], None],
    ) -> None:
        self._config = config
        self._halt = halt
        self._stats: dict[tuple[str, str], VersionStats] = {}
        self._windows: dict[tuple[str, str], deque[bool]] = {}
        self._lock = threading.Lock()

    def _entry(self, model: str, version: str) -> VersionStats:
        key = (model, version)
        if key not in self._stats:
            self._stats[key] = VersionStats()
        return self._stats[key]

    # ---- 计数记录（由 Gateway 在与整体统计相同的判定点调用） ----

    def record_request(self, model: str, version: str) -> None:
        with self._lock:
            self._entry(model, version).requests += 1

    def record_success(self, model: str, version: str) -> None:
        with self._lock:
            self._entry(model, version).succeeded += 1
            self._observe(model, version, ok=True)

    def record_failure(self, model: str, version: str) -> None:
        with self._lock:
            self._entry(model, version).failed += 1
            self._observe(model, version, ok=False)

    def record_rejected(self, model: str, version: str) -> None:
        with self._lock:
            self._entry(model, version).rejected += 1

    def record_fallback(self, model: str, version: str) -> None:
        with self._lock:
            self._entry(model, version).fallbacks += 1

    def record_inference(self, model: str, version: str) -> None:
        with self._lock:
            self._entry(model, version).real_inferences += 1

    def reattribute_inference(self, model: str, from_version: str, to_version: str) -> None:
        """降级成功时把推理归属从源版本改到实际产出结果的版本，保持合计守恒。"""
        with self._lock:
            src = self._entry(model, from_version)
            if src.real_inferences > 0:
                src.real_inferences -= 1
            self._entry(model, to_version).real_inferences += 1

    # ---- 自动止损 ----

    def _observe(self, model: str, version: str, ok: bool) -> None:
        """把一次成败纳入滑动窗口；达阈值则触发止损（只影响新请求）。"""
        spec = self._config.models.get(model)
        breaker = None
        if spec is not None:
            vspec = spec.versions.get(version)
            breaker = vspec.breaker if vspec is not None else None
        if breaker is None:
            return
        key = (model, version)
        window = self._windows.setdefault(key, deque(maxlen=breaker.window))
        window.append(ok)
        if len(window) < breaker.min_requests:
            return
        failures = window.count(False)
        if failures / len(window) >= breaker.failure_ratio:
            self._halt(model, version)

    # ---- 快照 ----

    def snapshot(self) -> dict[str, dict[str, int]]:
        """返回 {"model@version": {...}} 形式的分版本统计。"""
        with self._lock:
            return {
                f"{model}@{version}": stats.snapshot()
                for (model, version), stats in sorted(self._stats.items())
            }
