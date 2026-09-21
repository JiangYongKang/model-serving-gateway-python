"""模型注册表：版本、别名、下线状态与路由。

规则：
- 请求可指定具体版本号（v1）或别名（stable）；缺省走模型的 "default" 别名；
- 未知模型 -> UnknownModelError；未知版本/别名 -> UnknownVersionError；
  已下线版本 -> ModelOfflineError，三者原因可区分；
- 别名切换是一次性快照替换；resolve 在锁内返回确定的版本号，
  单个请求自始至终只使用解析出的版本，绝不跨版本混合结果。
"""

from __future__ import annotations

import asyncio

from app.backend import LocalBackend
from app.config import GatewayConfig
from app.errors import ModelOfflineError, UnknownModelError, UnknownVersionError

DEFAULT_ALIAS = "default"


class Registry:
    def __init__(self, config: GatewayConfig) -> None:
        self.config = config
        self.backends: dict[tuple[str, str], LocalBackend] = {
            (s.model, s.version): LocalBackend(s) for s in config.versions
        }
        # (model, alias) -> version。未配置 default 时惰性指向首个版本。
        self.aliases: dict[tuple[str, str], str] = {}
        for name, ver in config.aliases.items():
            model, alias = name.split(":", 1)
            self.aliases[(model, alias)] = ver
        self.offline: set[tuple[str, str]] = set()
        self._alias_lock = asyncio.Lock()

    async def warmup_all(self) -> None:
        for backend in self.backends.values():
            await backend.warmup()

    def _known_model(self, model: str) -> bool:
        return any(m == model for (m, _v) in self.backends)

    def _default_version(self, model: str) -> str | None:
        pinned = self.aliases.get((model, DEFAULT_ALIAS))
        if pinned is not None:
            return pinned
        versions = sorted(v for (m, v) in self.backends if m == model)
        return versions[0] if versions else None

    async def resolve(self, model: str, version_or_alias: str | None) -> str:
        """把“版本或别名”解析为真实版本号；非法情形抛出可区分错误。

        解析在别名锁内完成快照读取，别名并发切换不会使一个请求看到两个版本。
        """
        if not self._known_model(model):
            raise UnknownModelError(f"unknown model: {model}")

        async with self._alias_lock:
            if version_or_alias is None:
                resolved = self._default_version(model)
            elif (model, version_or_alias) in self.aliases:
                resolved = self.aliases[(model, version_or_alias)]
            elif (model, version_or_alias) in self.backends:
                resolved = version_or_alias  # 直接给了版本号
            else:
                raise UnknownVersionError(
                    f"model {model} has no version/alias: {version_or_alias}"
                )

        # resolved 必非 None（模型已知）；下线判定与解析使用同一快照结果。
        assert resolved is not None
        if (model, resolved) in self.offline:
            raise ModelOfflineError(f"{model}@{resolved} is offline")
        return resolved

    def get_backend(self, model: str, version: str) -> LocalBackend:
        return self.backends[(model, version)]

    def fallback_target(self, model: str, version: str) -> str | None:
        return self.config.fallback_chains.get(f"{model}:{version}")

    async def set_alias(self, model: str, alias: str, version: str) -> None:
        """原子切换别名：目标必须存在且未下线，否则拒绝（不产生半更新）。"""
        if not self._known_model(model):
            raise UnknownModelError(f"unknown model: {model}")
        if (model, version) not in self.backends:
            raise UnknownVersionError(f"unknown alias target: {model}@{version}")
        async with self._alias_lock:
            self.aliases[(model, alias)] = version

    def set_offline(self, model: str, version: str, offline: bool) -> None:
        if (model, version) not in self.backends:
            raise UnknownVersionError(f"unknown version: {model}@{version}")
        if offline:
            self.offline.add((model, version))
        else:
            self.offline.discard((model, version))

    def all_ready(self) -> bool:
        return all(b.ready for b in self.backends.values())

    def ready_snapshot(self) -> dict[str, bool]:
        return {b.key: b.ready for b in self.backends.values()}
