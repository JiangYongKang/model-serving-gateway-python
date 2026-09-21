"""确定性本地推理后端。

输出完全由 (model, version, 规范化输入) 决定，不依赖任何外部服务。
支持故障注入（恒失败 / 每个唯一输入首次尝试失败），用于真实触发重试。
"""

from __future__ import annotations

import asyncio
import hashlib

from app.config import VersionSpec
from app.errors import InferenceFailedError


class LocalBackend:
    """一个 (model, version) 对应一个本地后端实例。"""

    def __init__(self, spec: VersionSpec) -> None:
        self.spec = spec
        self.ready = False
        # 真实副作用计数：每处理一个输入 +1（含失败尝试，重试会再次计数）。
        self._calls = 0
        # 每个唯一输入的尝试序号（用于确定性的“首次失败”注入）。
        self._attempts: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def warmup(self) -> None:
        if self.spec.warmup:
            await asyncio.sleep(self.spec.warmup)
        # 本地后端的“真实可用性”就是它被成功构造并完成预热。
        self.ready = True

    @property
    def key(self) -> str:
        return f"{self.spec.model}@{self.spec.version}"

    async def infer(
        self, inputs: list[tuple[str, str]]
    ) -> list[tuple[str | None, BaseException | None]]:
        """批量推理。

        - 确定性：相同 (model, version, canonical_input) 必得相同输出；
        - 批内隔离：每个输入独立计算，单项失败不污染其他项；
        - 每处理一个输入即产生一次真实副作用（_calls + 1）。
        """
        # 模拟后端计算耗时；整批共享一次等待。
        if self.spec.latency:
            await asyncio.sleep(self.spec.latency)

        results: list[tuple[str | None, BaseException | None]] = []
        async with self._lock:
            for _request_id, canonical in inputs:
                self._calls += 1
                attempt = self._attempts.get(canonical, 0) + 1
                self._attempts[canonical] = attempt
                err = self._maybe_fail(canonical, attempt)
                if err is not None:
                    results.append((None, err))
                else:
                    digest = hashlib.sha256(
                        f"{self.spec.model}|{self.spec.version}|{canonical}".encode()
                    ).hexdigest()[:16]
                    results.append(
                        (f"{self.spec.model}/{self.spec.version}:{digest}", None)
                    )
        return results

    def _maybe_fail(self, canonical: str, attempt: int) -> BaseException | None:
        fault = self.spec.fault
        if fault == "always_fail":
            return InferenceFailedError(
                f"backend {self.key} injected always_fail"
            )
        if fault == "fail_first_attempt_per_input" and attempt == 1:
            return InferenceFailedError(
                f"backend {self.key} first-attempt failure for input {canonical[:8]}"
            )
        return None

    @property
    def call_count(self) -> int:
        return self._calls
