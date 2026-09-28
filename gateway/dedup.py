"""请求去重与用量统计。

相同 (model, request_id) 的提交（并发在途或已完成）只产生一次真实推理：
- 在途：跟随者等待领导者的同一个 future；
- 已完成：直接返回保存的结果（有界 LRU，容量 1024，确定性淘汰）；
- 失败结果同样被共享，重复提交不会触发新的真实推理；
- 作用域为 (model, request_id)：灰度加权分流可能把同一 request_id 的
  几次提交解析到不同版本，但只有第一次提交真实执行，其余提交共享同一
  结果，保证“同一 request_id 只真实推理一次、结果一致”在灰度分流、
  权重热更新与回退后都成立；
- 每条记录登记实际产生结果的版本（降级时为降级目标），回退按版本
  清除记录：被撤销版本产生的结果不再被返回；清除时同时摘除该版本
  在途条目的发布资格，避免领导者在回退后才把旧版本结果发布出去。
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

# work 返回 (结果, 实际产生结果的版本)
WorkResult = tuple[str, str]


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


@dataclass
class _Inflight:
    """在途去重条目：领导者 future + 发布令牌。

    回退按版本清除时会摘除令牌（token 置空）；领导者完成时若发现令牌已
    失效，就不再把结果发布为已完成记录（仍会唤醒其跟随者，让在途请求
    照常收尾）。
    """

    future: asyncio.Future
    initial_version: str
    token: object | None = None


# 已完成记录：(成功?, 值, 产生结果的版本)
_DoneRecord = tuple[bool, object, str]


class DedupRegistry:
    def __init__(self, capacity: int = 1024) -> None:
        self._capacity = capacity
        self._inflight: dict[tuple[str, str], _Inflight] = {}
        self._done: OrderedDict[tuple[str, str], _DoneRecord] = OrderedDict()
        self._lock = asyncio.Lock()

    async def run_once(
        self,
        model: str,
        request_id: str,
        initial_version: str,
        work: Callable[[], Awaitable[WorkResult]],
    ) -> tuple[str, bool]:
        """同一 (model, request_id) 只真实执行一次。

        返回 (结果, 是否本次真实执行)；失败时抛出共享的异常。
        initial_version 为本次入口解析出的版本，仅用于回退清除时定位
        在途条目；真实结果的归属版本以 work 返回值为准（含降级）。
        """
        key = (model, request_id)
        loop = asyncio.get_running_loop()
        async with self._lock:
            if key in self._done:
                ok, val, _producer = self._done[key]
                self._done.move_to_end(key)
                if ok:
                    return val, False  # type: ignore[return-value]
                raise val  # type: ignore[misc]
            entry = self._inflight.get(key)
            if entry is None:
                token = object()
                entry = _Inflight(
                    future=loop.create_future(),
                    initial_version=initial_version,
                    token=token,
                )
                self._inflight[key] = entry
                leader = True
            else:
                token = None
                leader = False

        if not leader:
            ok, val, _producer = await asyncio.shield(entry.future)
            if ok:
                return val, False  # type: ignore[return-value]
            raise val  # type: ignore[misc]

        try:
            result, producer_version = await work()
            outcome: _DoneRecord = (True, result, producer_version)
        except Exception as exc:  # 失败结果也共享；归属记到入口解析版本
            outcome = (False, exc, initial_version)
        async with self._lock:
            current = self._inflight.get(key)
            publish = current is entry and current.token is token
            if current is entry:
                self._inflight.pop(key, None)
            if publish:
                self._done[key] = outcome
                while len(self._done) > self._capacity:
                    self._done.popitem(last=False)
            entry.future.set_result(outcome)
        ok, val, _producer = outcome
        if ok:
            return val, True  # type: ignore[return-value]
        raise val  # type: ignore[misc]

    async def purge_versions(self, model: str, versions: list[str] | set[str]) -> int:
        """清除某模型下、由指定版本集合产生的去重记录。

        - 已完成记录：实际产生结果的版本命中集合即删除（在途请求不受影响，
          跟随者仍能拿到领导者结果正常收尾）；
        - 在途条目：入口解析版本命中集合的，摘除其发布令牌，使领导者完成后
          不把旧版本结果登记为可复用记录（回退后新的重复提交必须重新执行）。
        返回清除的已完成记录条数。
        """
        targets = set(versions)
        async with self._lock:
            keys = [k for k, (_ok, _val, producer) in self._done.items()
                    if k[0] == model and producer in targets]
            for k in keys:
                del self._done[k]
            for (m, _rid), entry in self._inflight.items():
                if m == model and entry.initial_version in targets:
                    entry.token = None
            return len(keys)
