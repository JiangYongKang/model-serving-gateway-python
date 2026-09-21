"""共享测试设施：可配置的网关工厂与日志。"""
from __future__ import annotations

import logging

import pytest

from gateway.config import (
    AdmissionConfig,
    BatchConfig,
    CacheConfig,
    GatewayConfig,
    ModelSpec,
    RetryConfig,
    VersionSpec,
)
from gateway.gateway import Gateway

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("test")


def make_config(
    *,
    batch: BatchConfig | None = None,
    admission: AdmissionConfig | None = None,
    cache: CacheConfig | None = None,
    retry: RetryConfig | None = None,
    fail_prompts: frozenset[str] = frozenset(),
) -> GatewayConfig:
    return GatewayConfig(
        models={
            "m": ModelSpec(
                name="m",
                versions={
                    "v1": VersionSpec(version="v1", latency_ms=5.0, fail_prompts=fail_prompts),
                    "v2": VersionSpec(version="v2", latency_ms=5.0),
                    "v0": VersionSpec(version="v0", latency_ms=5.0, retired=True),
                },
                aliases={"latest": "v1"},
                fallback=("v2",),
            ),
            "slow": ModelSpec(
                name="slow",
                versions={"v1": VersionSpec(version="v1", latency_ms=150.0)},
                aliases={"latest": "v1"},
            ),
        },
        batch=batch or BatchConfig(window_ms=20.0, max_batch_size=8, max_wait_ms=200.0),
        admission=admission or AdmissionConfig(),
        cache=cache or CacheConfig(capacity=4, enabled=True),
        retry=retry or RetryConfig(max_attempts=2),
    )


@pytest.fixture
async def gw() -> Gateway:
    g = Gateway(make_config())
    await g.start()
    yield g
    await g.shutdown()
