"""请求去重与用量统计。

相同 request_id 的提交（并发在途或已完成）只产生一次真实推理：
- 在途：跟随者等待领导者的同一个 future；
- 已完成：直接返回保存的结果（有界 LRU，容量 1024，确定性淘汰）；
- 失败结果同样被共享，重复提交不会触发新的真实推理。
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass


@dataclass
class UsageStats:
    """用量统计：accepted = succeeded + failed + rejected，守恒。"""

    accepted: int = 0
    succeeded: int = 0
    failed: int = 0
    rejected: int = 0
    cache_hits: int = 0
    dedup_hits: int = 0
    real_inferences: int = 0   # 真实推理次数（重试/降级/去重不重复计）
    fallbacks: int = 0         # 发生降级的请求数

    def snapshot(self) -> dict[str, int]:
        return dict(self.__dict__)


class DedupRegistry:
    def __init__(self, capacity: int = 1024) -> None:
        self._capacity = capacity
        self._inflight: dict[str, asyncio.Future] = {}
        self._done: OrderedDict[str, tuple[bool, object]] = OrderedDict()
        self._lock = asyncio.Lock()

    async def run_once(
        self,
        request_id: str,
        work: Callable[[], Awaitable[str]],
    ) -> tuple[str, bool]:
        """返回 (结果, 是否本次真实执行)；失败时抛出共享的异常。"""
        loop = asyncio.get_running_loop()
        async with self._lock:
            if request_id in self._done:
                ok, val = self._done[request_id]
                self._done.move_to_end(request_id)
                if ok:
                    return val, False  # type: ignore[return-value]
                raise val  # type: ignore[misc]
            fut = self._inflight.get(request_id)
            if fut is None:
                fut = loop.create_future()
                self._inflight[request_id] = fut
                leader = True
            else:
                leader = False

        if not leader:
            ok, val = await asyncio.shield(fut)
            if ok:
                return val, False
            raise val

        try:
            result = await work()
            outcome: tuple[bool, object] = (True, result)
        except Exception as exc:
            outcome = (False, exc)
        async with self._lock:
            self._inflight.pop(request_id, None)
            self._done[request_id] = outcome
            while len(self._done) > self._capacity:
                self._done.popitem(last=False)
            fut.set_result(outcome)
        ok, val = outcome
        if ok:
            return val, True  # type: ignore[return-value]
        raise val  # type: ignore[misc]
