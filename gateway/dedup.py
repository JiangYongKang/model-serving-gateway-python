"""请求去重与用量统计。

相同 (model, request_id) 的提交（并发在途或已完成）只产生一次真实推理，
**与提交落到哪个版本无关**——灰度分流、权重热更新期间同一 request_id 的
多次（含并发）提交必须得到同一个结果：
- 在途：跟随者等待领导者的同一个执行；领导者尚未结束时发生回退会封闭
  该在途记录（结果只交给已在等待的提交，之后的重复提交重新执行）；
- 已完成：直接返回保存的结果（有界 LRU，容量 1024，确定性淘汰）；
- 失败结果同样被共享，重复提交不会触发新的真实推理。

回退语义由“版本代际”支持：某版本被别名回退撤销时其代际 +1，并清除该
版本实际产生结果的已完成记录；以旧代际建立的在途记录完成后不再发布。
代际只在回退（语义边界变更）时推进，权重热更新不推进，因此灰度期间
同一 request_id 始终只真实推理一次。
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

Scope = tuple[str, str]  # (model, version)

# run_once 第三返回值：领导者（本次执行，结果允许发布/写缓存）、
# 跟随者（复用在途/已完成结果）、被回退封闭的领导者（本次执行但不发布）。
LEADER = "leader"
FOLLOWER = "follower"
SEALED = "sealed"


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
    """领导者在途执行：跟随者共享同一 future；epoch 为建立时的版本代际。"""

    future: asyncio.Future
    routed_version: str
    epoch: int


@dataclass
class _Outcome:
    """一次去重执行的终态。"""

    ok: bool
    val: object
    version: str   # 实际产生结果的版本（降级时可能不同于路由版本）
    epoch: int     # 领导者建立时其路由版本的代际


class DedupRegistry:
    """按 (model, request_id) 去重；版本代际保证回退后旧版本结果不再被返回。"""

    def __init__(self, capacity: int = 1024) -> None:
        self._capacity = capacity
        self._inflight: dict[tuple[str, str], _Inflight] = {}
        self._done: OrderedDict[tuple[str, str], _Outcome] = OrderedDict()
        # (model, version) -> 当前代际；回退撤销该版本时 +1
        self._epochs: dict[Scope, int] = {}
        self._lock = asyncio.Lock()

    def epoch(self, scope: Scope) -> int:
        return self._epochs.get(scope, 0)

    def _outcome_current(self, model: str, out: _Outcome) -> bool:
        """已完成记录是否仍可见：产生结果的版本代际未被回退推进。"""
        return self._epochs.get((model, out.version), 0) == out.epoch

    async def run_once(
        self,
        model: str,
        routed_version: str,
        request_id: str,
        work: Callable[[], Awaitable[tuple[str, str, bool]]],
    ) -> tuple[str, str, str]:
        """同一 (model, request_id) 只执行一次 work。

        work 返回 (结果, 实际产生结果的版本, 是否来自版本缓存)；降级时
        实际版本可能不同于路由版本。本方法返回 (结果, 实际版本, 角色)，
        角色为 LEADER / FOLLOWER / SEALED；失败时抛出共享的异常
        （异常对象上带实际版本，供统计与回退封闭判定）。
        """
        key = (model, request_id)
        loop = asyncio.get_running_loop()
        async with self._lock:
            done = self._done.get(key)
            if done is not None and self._outcome_current(model, done):
                self._done.move_to_end(key)
                if done.ok:
                    return done.val, done.version, FOLLOWER  # type: ignore[return-value]
                raise done.val  # type: ignore[misc]
            # 过期（回退撤销了产生结果的版本）的已完成记录立即删除
            if done is not None:
                self._done.pop(key, None)

            entry = self._inflight.get(key)
            if entry is not None and entry.epoch == self._epochs.get(
                (model, entry.routed_version), 0
            ):
                fut = entry.future
                join = True
            else:
                # 无在途，或旧在途已被回退封闭：本次提交成为新领导者
                fut = loop.create_future()
                entry = _Inflight(
                    future=fut,
                    routed_version=routed_version,
                    epoch=self._epochs.get((model, routed_version), 0),
                )
                self._inflight[key] = entry
                join = False

        if join:
            outcome: _Outcome = await asyncio.shield(fut)
            if outcome.ok:
                return outcome.val, outcome.version, FOLLOWER  # type: ignore[return-value]
            raise outcome.val  # type: ignore[misc]

        produced = routed_version
        try:
            result, produced, _from_cache = await work()
            outcome = _Outcome(True, result, produced, entry.epoch)
        except Exception as exc:
            produced = getattr(exc, "_dedup_version", produced)
            outcome = _Outcome(False, exc, produced, entry.epoch)

        publish = False
        async with self._lock:
            if self._inflight.get(key) is entry:
                self._inflight.pop(key, None)
                # 仅当回退未撤销相关版本时才发布为可复用的已完成记录：
                # 路由版本或实际产生结果的版本代际被推进，都意味着该结果
                # 不再允许作为重复提交结果返回。
                if (
                    self._epochs.get((model, routed_version), 0) == entry.epoch
                    and self._epochs.get((model, produced), 0) == entry.epoch
                ):
                    self._done[key] = outcome
                    while len(self._done) > self._capacity:
                        self._done.popitem(last=False)
                    publish = True
            if not fut.done():
                fut.set_result(outcome)
        if outcome.ok:
            return outcome.val, outcome.version, LEADER if publish else SEALED  # type: ignore[return-value]
        raise outcome.val  # type: ignore[misc]

    async def revoke_version(self, scope: Scope) -> int:
        """回退撤销版本：代际 +1（封闭在途记录），清除该版本产生的已完成记录。

        返回清除的已完成记录条数。
        """
        model, version = scope
        async with self._lock:
            self._epochs[scope] = self._epochs.get(scope, 0) + 1
            keys = [
                k for k, o in self._done.items() if k[0] == model and o.version == version
            ]
            for k in keys:
                del self._done[k]
            return len(keys)
