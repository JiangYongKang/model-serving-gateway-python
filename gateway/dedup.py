"""请求去重与用量统计。"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field


@dataclass
class UsageStats:
    """用量统计：并发、失败与降级场景下守恒。"""

    accepted: int = 0        # 受理请求数
    succeeded: int = 0       # 成功数（含缓存命中与去重命中）
    failed: int = 0          # 失败数
    rejected: int = 0        # 被拒绝数（背压/停机/未就绪）
    cache_hits: int = 0
    dedup_hits: int = 0      # 去重命中（共享他人结果）
    real_inferences: int = 0  # 真实推理次数（重试不重复计）
    fallbacks: int = 0       # 发生降级的请求数

    def snapshot(self) -> dict[str, int]:
        return dict(self.__dict__)


class DedupRegistry:
    """相同请求标识的并发提交只产生一次真实推理。"""

    async def run_once(
        self,
        request_id: str,
        work: Callable[[], Awaitable[str]],
    ) -> tuple[str, bool]:
        """执行或等待同标识请求；返回 (结果, 是否本次真实执行)。"""
        raise NotImplementedError
