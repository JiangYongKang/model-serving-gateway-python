"""动态合批器：按 (模型, 版本) 分桶，窗口/批大小/等待上限可配置。"""
from __future__ import annotations

from collections.abc import Awaitable, Callable

from gateway.config import BatchConfig


class BatchScheduler:
    """同模型同版本的请求在窗口内合并成批；批内单请求失败不污染其他请求。"""

    def __init__(self, config: BatchConfig) -> None:
        self._config = config

    async def submit(
        self,
        key: tuple[str, str],
        prompt: str,
        infer: Callable[[list[str]], Awaitable[list[str]]],
    ) -> str:
        """提交单条请求，返回该请求的结果。"""
        raise NotImplementedError

    async def shutdown(self) -> None:
        """停止调度，等待在途批次完成。"""
        raise NotImplementedError
