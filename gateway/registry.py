"""模型/版本/别名路由注册表。"""
from __future__ import annotations

from gateway.config import GatewayConfig, ModelSpec, VersionSpec


class Registry:
    """按模型与版本或别名解析到具体版本；别名解析对单请求原子。"""

    def __init__(self, config: GatewayConfig) -> None:
        self._config = config

    def resolve(self, model: str, version_or_alias: str | None) -> tuple[ModelSpec, VersionSpec]:
        """解析请求目标，返回 (模型, 版本)；失败抛出带原因码的 GatewayError。"""
        raise NotImplementedError

    def set_alias(self, model: str, alias: str, version: str) -> None:
        """切换别名指向（运行期）。"""
        raise NotImplementedError

    def retire_version(self, model: str, version: str) -> None:
        """下线某版本。"""
        raise NotImplementedError
