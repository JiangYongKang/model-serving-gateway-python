"""配置类型与默认配置。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True)
class VersionSpec:
    model: str
    version: str
    latency: float = 0.0
    # 故障注入：none / always_fail / fail_first_attempt_per_input
    # fail_first_attempt_per_input：每个唯一输入的首次尝试失败、重试成功，
    # 行为按输入确定，与批内到达顺序无关。
    fault: Literal["none", "always_fail", "fail_first_attempt_per_input"] = "none"
    warmup: float = 0.0


@dataclass(frozen=True)
class BatchConfig:
    window: float = 0.02          # 合批窗口（秒）
    max_size: int = 32           # 单批最大请求数
    max_wait: float = 1.0        # 单请求等待上限（秒）
    max_inflight_batches: int = 1  # 每个 (模型,版本) 同时在执行的批数上限


@dataclass(frozen=True)
class ResourceConfig:
    global_concurrency: int = 8
    global_queue: int = 64
    per_model_concurrency: int = 2
    per_model_queue: int = 16
    acquire_timeout: float = 2.0


@dataclass(frozen=True)
class RetryConfig:
    max_retries: int = 1
    backoff: float = 0.0
    fallback_alias: str | None = None


@dataclass(frozen=True)
class CacheConfig:
    capacity: int = 128
    enabled: bool = True
    ttl: float = 0.0  # 0 表示不过期（本实现用容量淘汰，TTL 保留配置位）


@dataclass
class GatewayConfig:
    versions: list[VersionSpec] = field(default_factory=list)
    aliases: dict[str, str] = field(default_factory=dict)
    fallback_chains: dict[str, str] = field(default_factory=dict)
    batch: BatchConfig = field(default_factory=BatchConfig)
    resources: ResourceConfig = field(default_factory=ResourceConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    shutdown_timeout: float = 5.0


def default_config() -> GatewayConfig:
    """演示配置：多模型、多版本、别名、故障注入与降级链。

    - math: v1/v2 正常，v3 首次调用失败随后成功（验证重试）；
      别名 stable->v1, canary->v2, edge->v3。
    - flaky: v1 恒失败、v2 正常，v1 配置降级到 v2（验证降级成功）。
    - doom: v1 恒失败，降级目标 v2 未注册（验证降级不可用 -> 明确失败）。
    - slow / fast: 用于验证慢模型不阻塞其他模型。
    """
    versions = [
        VersionSpec(model="math", version="v1", latency=0.01),
        VersionSpec(model="math", version="v2", latency=0.01),
        VersionSpec(
            model="math", version="v3", latency=0.01,
            fault="fail_first_attempt_per_input",
        ),
        VersionSpec(model="slow", version="v1", latency=0.25),
        VersionSpec(model="fast", version="v1", latency=0.0),
        VersionSpec(model="flaky", version="v1", latency=0.01, fault="always_fail"),
        VersionSpec(model="flaky", version="v2", latency=0.01),
        VersionSpec(model="doom", version="v1", latency=0.01, fault="always_fail"),
    ]
    aliases = {
        "math:stable": "v1",
        "math:canary": "v2",
        "math:edge": "v3",
    }
    fallback_chains = {
        "flaky:v1": "v2",
        "doom:v1": "v2",
    }
    return GatewayConfig(
        versions=versions,
        aliases=aliases,
        fallback_chains=fallback_chains,
        batch=BatchConfig(window=0.05, max_size=8, max_wait=1.0),
        resources=ResourceConfig(
            global_concurrency=8, global_queue=64,
            per_model_concurrency=2, per_model_queue=8,
            acquire_timeout=2.0,
        ),
        retry=RetryConfig(max_retries=2, backoff=0.0),
        cache=CacheConfig(capacity=16),
        shutdown_timeout=5.0,
    )
