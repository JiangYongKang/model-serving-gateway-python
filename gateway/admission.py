"""资源隔离与背压：全局及每模型的并发与排队上限。"""
from __future__ import annotations

from gateway.config import AdmissionConfig


class Admission:
    """信号量 + 有界排队；超限或超时按策略明确拒绝。"""

    def __init__(self, config: AdmissionConfig) -> None:
        self._config = config

    async def acquire(self, model: str) -> None:
        """获取执行许可；超限抛 QUEUE_FULL，超时抛 WAIT_TIMEOUT。"""
        raise NotImplementedError

    def release(self, model: str) -> None:
        """释放执行许可。"""
        raise NotImplementedError
