"""模型/版本/别名路由注册表。

解析规则：
- 请求可指定精确版本号或别名；精确版本优先于同名别名；
- 未指定时使用别名 "latest"；
- 未知模型 / 未知版本(或别名) / 已下线版本分别给出可区分原因码；
- 单请求只在入口解析一次别名，之后固定使用该版本，绝不跨版本混合。
"""
from __future__ import annotations

import threading

from gateway.config import GatewayConfig, ModelSpec, VersionSpec
from gateway.errors import GatewayError, Reason


class Registry:
    def __init__(self, config: GatewayConfig) -> None:
        self._models: dict[str, ModelSpec] = dict(config.models)
        # 运行期可变状态独立保存，避免修改冻结配置
        self._aliases: dict[str, dict[str, str]] = {
            name: dict(spec.aliases) for name, spec in config.models.items()
        }
        self._retired: set[tuple[str, str]] = {
            (name, v)
            for name, spec in config.models.items()
            for v, vspec in spec.versions.items()
            if vspec.retired
        }
        self._lock = threading.Lock()

    def resolve(self, model: str, version_or_alias: str | None) -> tuple[ModelSpec, VersionSpec]:
        spec = self._models.get(model)
        if spec is None:
            raise GatewayError(Reason.UNKNOWN_MODEL, f"unknown model: {model!r}")

        key = version_or_alias if version_or_alias is not None else "latest"
        with self._lock:
            aliases = dict(self._aliases.get(model, {}))
            retired = set(self._retired)

        if key in spec.versions:
            version = key
        elif key in aliases:
            version = aliases[key]
        elif version_or_alias is None:
            raise GatewayError(Reason.UNKNOWN_ALIAS, f"model {model!r} has no 'latest' alias")
        else:
            raise GatewayError(Reason.UNKNOWN_VERSION, f"unknown version or alias: {key!r}")

        vspec = spec.versions.get(version)
        if vspec is None:
            raise GatewayError(
                Reason.UNKNOWN_VERSION, f"alias {key!r} points to missing version {version!r}"
            )
        if (model, version) in retired:
            raise GatewayError(Reason.VERSION_RETIRED, f"version retired: {model}@{version}")
        return spec, vspec

    def is_retired(self, model: str, version: str) -> bool:
        with self._lock:
            return (model, version) in self._retired

    def set_alias(self, model: str, alias: str, version: str) -> None:
        if model not in self._models:
            raise GatewayError(Reason.UNKNOWN_MODEL, f"unknown model: {model!r}")
        if version not in self._models[model].versions:
            raise GatewayError(Reason.UNKNOWN_VERSION, f"unknown version: {version!r}")
        with self._lock:
            self._aliases.setdefault(model, {})[alias] = version

    def retire_version(self, model: str, version: str) -> None:
        if model not in self._models:
            raise GatewayError(Reason.UNKNOWN_MODEL, f"unknown model: {model!r}")
        if version not in self._models[model].versions:
            raise GatewayError(Reason.UNKNOWN_VERSION, f"unknown version: {version!r}")
        with self._lock:
            self._retired.add((model, version))
