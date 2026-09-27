"""模型/版本/别名路由注册表。

解析规则：
- 请求可指定精确版本号或别名；精确版本优先于同名别名；
- 未指定时使用别名 "latest"；
- 别名可指向多个版本并按权重分流（灰度放量）；权重为 0、已下线或
  已触发止损的版本不参与分流；单请求只在入口解析一次，之后固定使用
  该版本，绝不跨版本混合；
- 未知模型 / 未知版本(或别名) / 已下线版本分别给出可区分原因码。
"""
from __future__ import annotations

import random
import threading

from gateway.config import GatewayConfig, ModelSpec, VersionSpec
from gateway.errors import GatewayError, Reason


class Registry:
    def __init__(self, config: GatewayConfig) -> None:
        self._models: dict[str, ModelSpec] = dict(config.models)
        # 运行期可变状态独立保存，避免修改冻结配置
        # alias -> {version: weight}；旧式 aliases: {alias: version} 等价于权重 1
        self._alias_weights: dict[str, dict[str, dict[str, float]]] = {}
        for name, spec in config.models.items():
            weights: dict[str, dict[str, float]] = {
                alias: {version: 1.0} for alias, version in spec.aliases.items()
            }
            for alias, wmap in spec.alias_weights.items():
                weights[alias] = dict(wmap)
            self._alias_weights[name] = weights
        self._retired: set[tuple[str, str]] = {
            (name, v)
            for name, spec in config.models.items()
            for v, vspec in spec.versions.items()
            if vspec.retired
        }
        self._tripped: set[tuple[str, str]] = set()  # 触发自动止损的版本
        self._lock = threading.Lock()
        self._rng = random.Random()

    def resolve(self, model: str, version_or_alias: str | None) -> tuple[ModelSpec, VersionSpec]:
        spec = self._models.get(model)
        if spec is None:
            raise GatewayError(Reason.UNKNOWN_MODEL, f"unknown model: {model!r}")

        key = version_or_alias if version_or_alias is not None else "latest"
        with self._lock:
            if key in spec.versions:
                version = key
            elif key in self._alias_weights.get(model, {}):
                version = self._pick_weighted(model, key)
            elif version_or_alias is None:
                raise GatewayError(Reason.UNKNOWN_ALIAS, f"model {model!r} has no 'latest' alias")
            else:
                raise GatewayError(Reason.UNKNOWN_VERSION, f"unknown version or alias: {key!r}")
            retired = (model, version) in self._retired

        vspec = spec.versions.get(version)
        if vspec is None:
            raise GatewayError(
                Reason.UNKNOWN_VERSION, f"alias {key!r} points to missing version {version!r}"
            )
        if retired:
            raise GatewayError(Reason.VERSION_RETIRED, f"version retired: {model}@{version}")
        return spec, vspec

    def _pick_weighted(self, model: str, alias: str) -> str:
        """在锁内按权重选一个可用版本；权重 0 / 已下线 / 已止损版本不参与。"""
        weights = self._alias_weights[model][alias]
        positive = [(v, w) for v, w in weights.items() if w > 0]
        if not positive:
            raise GatewayError(
                Reason.UNKNOWN_VERSION, f"alias {alias!r} on {model!r} has no available version"
            )
        candidates = [
            (v, w)
            for v, w in positive
            if (model, v) not in self._retired and (model, v) not in self._tripped
        ]
        if not candidates:
            if any((model, v) in self._retired for v, _ in positive):
                raise GatewayError(
                    Reason.VERSION_RETIRED, f"alias {alias!r} points to retired version on {model!r}"
                )
            raise GatewayError(
                Reason.VERSION_TRIPPED, f"all versions of alias {alias!r} on {model!r} tripped"
            )
        versions, ws = zip(*candidates)
        return self._rng.choices(versions, weights=ws, k=1)[0]

    def is_retired(self, model: str, version: str) -> bool:
        with self._lock:
            return (model, version) in self._retired

    def set_alias(self, model: str, alias: str, version: str) -> None:
        """兼容接口：等价于把别名收敛为单版本（权重 1）。"""
        self.set_alias_weights(model, alias, {version: 1.0})

    def set_alias_weights(self, model: str, alias: str, weights: dict[str, float]) -> None:
        """热更新别名下的版本组成与权重；新到达的请求立即按最新配置分流。"""
        if model not in self._models:
            raise GatewayError(Reason.UNKNOWN_MODEL, f"unknown model: {model!r}")
        for version, w in weights.items():
            if version not in self._models[model].versions:
                raise GatewayError(Reason.UNKNOWN_VERSION, f"unknown version: {version!r}")
            if w < 0:
                raise GatewayError(Reason.UNKNOWN_VERSION, f"negative weight for {version!r}")
        with self._lock:
            self._alias_weights.setdefault(model, {})[alias] = dict(weights)

    def alias_targets(self, model: str, alias: str) -> dict[str, float]:
        with self._lock:
            return dict(self._alias_weights.get(model, {}).get(alias, {}))

    def trip_version(self, model: str, version: str) -> None:
        """自动止损：标记版本并从该模型所有别名的分流中摘除（其余版本继续服务）。"""
        with self._lock:
            self._tripped.add((model, version))

    def reset_trip(self, model: str, version: str) -> None:
        with self._lock:
            self._tripped.discard((model, version))

    def is_tripped(self, model: str, version: str) -> bool:
        with self._lock:
            return (model, version) in self._tripped

    def retire_version(self, model: str, version: str) -> None:
        if model not in self._models:
            raise GatewayError(Reason.UNKNOWN_MODEL, f"unknown model: {model!r}")
        if version not in self._models[model].versions:
            raise GatewayError(Reason.UNKNOWN_VERSION, f"unknown version: {version!r}")
        with self._lock:
            self._retired.add((model, version))

    def rollback(self, model: str, alias: str, version: str) -> list[str]:
        """一键回退：别名收敛为单版本，其余在量版本被撤销（下线）并返回列表。

        在途请求不受影响（它们已完成路由解析）；被撤销版本不再接受新请求。
        """
        if model not in self._models:
            raise GatewayError(Reason.UNKNOWN_MODEL, f"unknown model: {model!r}")
        if version not in self._models[model].versions:
            raise GatewayError(Reason.UNKNOWN_VERSION, f"unknown version: {version!r}")
        with self._lock:
            if (model, version) in self._retired:
                raise GatewayError(Reason.VERSION_RETIRED, f"version retired: {model}@{version}")
            current = self._alias_weights.setdefault(model, {}).get(alias, {})
            revoked = [v for v, w in current.items() if w > 0 and v != version]
            self._alias_weights[model][alias] = {version: 1.0}
            for v in revoked:
                self._retired.add((model, v))
                self._tripped.discard((model, v))
            return revoked
