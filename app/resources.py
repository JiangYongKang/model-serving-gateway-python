"""资源隔离与背压：全局/每模型两级并发与有界排队。

- 并发上限：asyncio.Semaphore（全局 + 每模型，需同时持有）；
- 排队上限：仅统计“等待者”，超限时立即 QueueFullError，不积压；
- 等待上限：acquire 超时 -> WaitTimeoutError；
- 慢模型占满自己的模型槽位不影响其他模型（各自独立槽位与队列）。
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass

from app.config import ResourceConfig
from app.errors import QueueFullError, WaitTimeoutError


@dataclass
class _Slot:
    concurrency: int
    queue_cap: int
    sem: asyncio.Semaphore
    active: int = 0   # 已持槽
    waiting: int = 0  # 纯排队等待

    @classmethod
    def make(cls, concurrency: int, queue_cap: int) -> "_Slot":
        return cls(concurrency, queue_cap, asyncio.Semaphore(concurrency))


class ResourcePool:
    def __init__(self, config: ResourceConfig) -> None:
        self.config = config
        self._global = _Slot.make(config.global_concurrency, config.global_queue)
        self._per_model: dict[str, _Slot] = {}

    def _model_slot(self, model: str) -> _Slot:
        slot = self._per_model.get(model)
        if slot is None:
            slot = _Slot.make(
                self.config.per_model_concurrency,
                self.config.per_model_queue,
            )
            self._per_model[model] = slot
        return slot

    async def _enter(self, slot: _Slot, label: str) -> bool:
        """进入一级槽位；返回 True 表示已持槽，False 表示因排队满被快速拒绝。

        在途总量 = active（已持槽）+ waiting（纯等待）。该总量达到
        “并发 + 排队”上限即快速拒绝，保证内存与等待者都有界。
        """
        if slot.active + slot.waiting >= slot.concurrency + slot.queue_cap:
            raise QueueFullError(
                f"{label} queue full: active={slot.active} "
                f"waiting={slot.waiting} cap={slot.concurrency + slot.queue_cap}"
            )
        slot.waiting += 1
        try:
            try:
                await asyncio.wait_for(
                    slot.sem.acquire(), timeout=self.config.acquire_timeout
                )
            except TimeoutError as e:
                raise WaitTimeoutError(
                    f"timed out waiting {label} slot"
                ) from e
        finally:
            slot.waiting -= 1
        slot.active += 1
        return True

    @staticmethod
    def _leave(slot: _Slot) -> None:
        slot.active -= 1
        slot.sem.release()

    @asynccontextmanager
    async def acquire(self, model: str):
        """依次获取模型级、全局级槽位；排队超限/等待超时明确拒绝。"""
        model_slot = self._model_slot(model)
        await self._enter(model_slot, f"per-model:{model}")
        got_model = True
        got_global = False
        try:
            try:
                await self._enter(self._global, "global")
            except BaseException:
                self._leave(model_slot)
                got_model = False
                raise
            got_global = True
            try:
                yield
            finally:
                self._leave(self._global)
        finally:
            if got_model:
                self._leave(model_slot)

    def utilization(self) -> dict[str, object]:
        return {
            "global": {"active": self._global.active, "waiting": self._global.waiting},
            "per_model": {
                m: {"active": s.active, "waiting": s.waiting}
                for m, s in self._per_model.items()
            },
        }
