"""网关编排：路由 → 缓存 → 去重 → 背压 → 合批 → 重试/降级 → 统计。"""
from __future__ import annotations

from gateway.config import GatewayConfig
from gateway.dedup import UsageStats


class Gateway:
    """本地模型推理网关主入口。"""

    def __init__(self, config: GatewayConfig) -> None:
        self._config = config

    async def start(self) -> None:
        """启动预热：就绪状态反映后端真实可用性。"""
        raise NotImplementedError

    async def shutdown(self) -> None:
        """优雅停机：在途请求完成或明确拒绝。"""
        raise NotImplementedError

    @property
    def ready(self) -> bool:
        raise NotImplementedError

    async def infer(
        self,
        model: str,
        prompt: str,
        version_or_alias: str | None = None,
        request_id: str | None = None,
    ) -> str:
        """推理入口：返回结果文本；失败抛带原因码的 GatewayError。"""
        raise NotImplementedError

    @property
    def stats(self) -> UsageStats:
        raise NotImplementedError
