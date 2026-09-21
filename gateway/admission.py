"""资源隔离与背压：全局及每模型的并发与排队上限。

- 并发槽满时进入有界排队；排队也满则立即以 QUEUE_FULL 拒绝；
- 排队超过 wait_timeout_ms 以 WAIT_TIMEOUT 拒绝；
- 计数有界，积压不会无界增长内存；每模型独立计数，慢模型不阻塞其他模型。
"""
from __future__ import annotations

import asyncio
import time

from gateway.config import AdmissionConfig
from gateway.errors import GatewayError, Reason


class Admission:
    def __init__(self, config: AdmissionConfig) -> None:
        self._cfg = config
        self._cond = asyncio.Condition()
        self._global_running = 0
        self._global_waiting = 0
        self._per_model: dict[str, list[int]] = {}  # model -> [running, waiting]

    def _model_counters(self, model: str) -> list[int]:
        return self._per_model.setdefault(model, [0, 0])

    def _slot_available(self, mc: list[int]) -> bool:
        return (
            self._global_running < self._cfg.global_concurrency
            and mc[0] < self._cfg.per_model_concurrency
        )

    async def acquire(self, model: str) -> None:
        cfg = self._cfg
        deadline = time.monotonic() + cfg.wait_timeout_ms / 1000.0
        async with self._cond:
            mc = self._model_counters(model)
            if self._slot_available(mc):
                self._global_running += 1
                mc[0] += 1
                return
            if self._global_waiting >= cfg.global_queue or mc[1] >= cfg.per_model_queue:
                raise GatewayError(Reason.QUEUE_FULL, f"queue full for model {model!r}")
            self._global_waiting += 1
            mc[1] += 1
            try:
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise GatewayError(Reason.WAIT_TIMEOUT, f"wait timeout for model {model!r}")
                    try:
                        await asyncio.wait_for(self._cond.wait(), remaining)
                    except asyncio.TimeoutError:
                        raise GatewayError(Reason.WAIT_TIMEOUT, f"wait timeout for model {model!r}")
                    if self._slot_available(mc):
                        self._global_running += 1
                        mc[0] += 1
                        return
            finally:
                self._global_waiting -= 1
                mc[1] -= 1

    async def release(self, model: str) -> None:
        async with self._cond:
            mc = self._model_counters(model)
            self._global_running -= 1
            mc[0] -= 1
            self._cond.notify_all()
