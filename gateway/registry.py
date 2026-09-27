"""模型/版本/别名路由注册表。

解析规则：
- 请求可指定精确版本号或别名；精确版本优先于同名别名；
- 未指定时使用别名 "latest"；
- 别名可指向多个版本并按权重分流；权重为 0、已下线或已止损的版本不参与分流；
- 未知模型 / 未知版本(或别名) / 已下线版本 / 无可用版本分别给出可区分原因码；
- 单请求只在入口解析一次别名，之后固定使用该版本，绝不跨版本混合。
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
        # 别名 -> {版本: 权重}；静态配置中的单版本别名等价于权重 1
        self._alias_weights: dict[str, dict[str, dict[str, float]]] = {
            name: {alias: {version: 1.0} for alias, version in spec.aliases.items()}
            for name, spec in config.models.items()
        }
        self._retired: set[tuple[str, str]] = {
            (name, v)
            for name, spec in config.models.items()
            for v, vspec in spec.versions.items()
            if vspec.retired
        }
        self._halted: set[tuple[str, str]] = set()  # 被自动止损停止放量的版本
        self._lock = threading.Lock()

    def resolve(self, model: str, version_or_alias: str | None) -> tuple[ModelSpec, VersionSpec]:
        spec = self._models.get(model)
        if spec is None:
            raise GatewayError(Reason.UNKNOWN_MODEL, f"unknown model: {model!r}")

        key = version_or_alias if version_or_alias is not None else "latest"
        with self._lock:
            weights = dict(self._alias_weights.get(model, {}).get(key, {}))
            retired = set(self._retired)
            halted = set(self._halted)

        if key in spec.versions:
            version = key
        elif weights:
            version = self._pick(model, key, weights, retired, halted)
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

    @staticmethod
    def _pick(
        model: str,
        alias: str,
        weights: dict[str, float],
        retired: set[tuple[str, str]],
        halted: set[tuple[str, str]],
    ) -> str:
        """按权重随机选择一个可用版本；权重为 0 / 已下线 / 已止损的版本不参与。"""
        eligible = {
            v: w
            for v, w in weights.items()
            if w > 0 and (model, v) not in retired and (model, v) not in halted
        }
        if not eligible:
            candidates = [v for v, w in weights.items() if w > 0]
            if candidates and all((model, v) in retired for v in candidates):
                # 兼容单版本别名的旧语义：指向的版本已下线
                raise GatewayError(
                    Reason.VERSION_RETIRED,
                    f"all versions of alias {alias!r} on model {model!r} are retired",
                )
            raise GatewayError(
                Reason.NO_ACTIVE_VERSION,
                f"alias {alias!r} on model {model!r} has no active version",
            )
        total = sum(eligible.values())
        r = random.random() * total
        chosen = next(iter(eligible))
        for v, w in eligible.items():
            r -= w
            if r < 0:
                chosen = v
                break
        return chosen

    def is_retired(self, model: str, version: str) -> bool:
        with self._lock:
            return (model, version) in self._retired

    def is_halted(self, model: str, version: str) -> bool:
        with self._lock:
            return (model, version) in self._halted

    def halt_version(self, model: str, version: str) -> None:
        """自动止损：停止该版本的放量（不再参与任何别名的分流）。"""
        with self._lock:
            self._halted.add((model, version))

    def set_alias(self, model: str, alias: str, version: str) -> None:
        """单版本别名（兼容旧配置）：等价于权重 {version: 1}。"""
        self.set_alias_weights(model, alias, {version: 1.0})

    def set_alias_weights(self, model: str, alias: str, weights: dict[str, float]) -> None:
        """灰度放量：让别名同时指向多个版本并按权重分流，可随时热更新。

        注意：调整权重不会解除已止损版本的止损状态（防止无意恢复放量），
        明确恢复请用 resume_version；一键回退用 rollback。
        """
        if model not in self._models:
            raise GatewayError(Reason.UNKNOWN_MODEL, f"unknown model: {model!r}")
        for version, w in weights.items():
            if version not in self._models[model].versions:
                raise GatewayError(Reason.UNKNOWN_VERSION, f"unknown version: {version!r}")
            if w < 0:
                raise GatewayError(Reason.UNKNOWN_VERSION, f"negative weight for {version!r}")
        with self._lock:
            self._alias_weights.setdefault(model, {})[alias] = dict(weights)

    def resume_version(self, model: str, version: str) -> None:
        """明确恢复被止损版本的放量。"""
        with self._lock:
            self._halted.discard((model, version))

    def rollback(self, model: str, alias: str, version: str) -> list[str]:
        """一键回退：把别名收敛回只由 version 承接，返回被撤销的版本列表。"""
        if model not in self._models:
            raise GatewayError(Reason.UNKNOWN_MODEL, f"unknown model: {model!r}")
        if version not in self._models[model].versions:
            raise GatewayError(Reason.UNKNOWN_VERSION, f"unknown version: {version!r}")
        with self._lock:
            old = self._alias_weights.setdefault(model, {}).get(alias, {})
            revoked = [v for v, w in old.items() if w > 0 and v != version]
            self._alias_weights[model][alias] = {version: 1.0}
            self._halted.discard((model, version))  # 明确回退目标视为运维意图，解除止损
        return revoked

    def alias_weights(self, model: str, alias: str) -> dict[str, float]:
        with self._lock:
            return dict(self._alias_weights.get(model, {}).get(alias, {}))

    def retire_version(self, model: str, version: str) -> None:
        if model not in self._models:
            raise GatewayError(Reason.UNKNOWN_MODEL, f"unknown model: {model!r}")
        if version not in self._models[model].versions:
            raise GatewayError(Reason.UNKNOWN_VERSION, f"unknown version: {version!r}")
        with self._lock:
            self._retired.add((model, version))
