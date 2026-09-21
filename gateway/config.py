"""网关配置：模型注册表、合批、背压、缓存、重试与降级。"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class VersionSpec:
    """单个模型版本的后端行为描述（确定性模拟）。"""

    version: str
    latency_ms: float = 5.0          # 模拟推理延迟
    retired: bool = False            # 是否已下线
    fail_rate: float = 0.0           # 模拟后端故障概率（0~1，确定性注入用）
    fail_prompts: frozenset[str] = frozenset()  # 对特定输入确定性失败


@dataclass(frozen=True)
class ModelSpec:
    """模型：若干版本 + 别名 + 降级链。"""

    name: str
    versions: dict[str, VersionSpec] = field(default_factory=dict)
    aliases: dict[str, str] = field(default_factory=dict)   # alias -> version
    fallback: tuple[str, ...] = ()  # 降级目标版本（按序尝试）


@dataclass(frozen=True)
class BatchConfig:
    """动态合批参数。"""

    window_ms: float = 10.0     # 合批窗口
    max_batch_size: int = 8     # 批大小上限
    max_wait_ms: float = 500.0  # 单请求等待上限


@dataclass(frozen=True)
class AdmissionConfig:
    """资源隔离与背压参数。"""

    global_concurrency: int = 32        # 全局并发上限
    global_queue: int = 64              # 全局排队上限
    per_model_concurrency: int = 8      # 每模型并发上限
    per_model_queue: int = 16           # 每模型排队上限
    wait_timeout_ms: float = 1000.0     # 排队等待超时


@dataclass(frozen=True)
class CacheConfig:
    """结果缓存参数。"""

    capacity: int = 256     # 容量上限，LRU 确定性淘汰
    enabled: bool = True


@dataclass(frozen=True)
class RetryConfig:
    """重试参数（重试不重复计用量）。"""

    max_attempts: int = 2


@dataclass(frozen=True)
class GatewayConfig:
    models: dict[str, ModelSpec] = field(default_factory=dict)
    batch: BatchConfig = field(default_factory=BatchConfig)
    admission: AdmissionConfig = field(default_factory=AdmissionConfig)
    cache: CacheConfig = field(default_factory=CacheConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
