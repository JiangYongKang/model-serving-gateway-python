"""演示配置：两个模型、多版本、别名与降级链，全部本地确定性运行。"""
from __future__ import annotations

from gateway.config import (
    AdmissionConfig,
    BatchConfig,
    CacheConfig,
    GatewayConfig,
    ModelSpec,
    RetryConfig,
    VersionSpec,
)


def demo_config() -> GatewayConfig:
    return GatewayConfig(
        models={
            "echo": ModelSpec(
                name="echo",
                versions={
                    "v1": VersionSpec(version="v1", latency_ms=5.0),
                    "v2": VersionSpec(version="v2", latency_ms=5.0),
                    "v0": VersionSpec(version="v0", latency_ms=5.0, retired=True),
                },
                aliases={"latest": "v2", "stable": "v1"},
                fallback=("v1",),
            ),
            "summarizer": ModelSpec(
                name="summarizer",
                versions={
                    "v1": VersionSpec(version="v1", latency_ms=20.0),
                },
                aliases={"latest": "v1"},
            ),
        },
        batch=BatchConfig(window_ms=10.0, max_batch_size=8, max_wait_ms=200.0),
        admission=AdmissionConfig(
            global_concurrency=32,
            global_queue=64,
            per_model_concurrency=8,
            per_model_queue=16,
            wait_timeout_ms=1000.0,
        ),
        cache=CacheConfig(capacity=256, enabled=True),
        retry=RetryConfig(max_attempts=2),
    )
