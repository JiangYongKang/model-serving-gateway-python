"""错误类型与可区分原因码。"""
from __future__ import annotations

import enum


class Reason(enum.Enum):
    """请求失败/拒绝的可区分原因码。"""

    UNKNOWN_MODEL = "unknown_model"            # 模型不存在
    UNKNOWN_VERSION = "unknown_version"        # 版本不存在
    VERSION_RETIRED = "version_retired"        # 版本已下线
    UNKNOWN_ALIAS = "unknown_alias"            # 别名不存在
    NO_ACTIVE_VERSION = "no_active_version"    # 别名下无可用版本（权重为 0 / 已止损 / 已下线）
    QUEUE_FULL = "queue_full"                  # 排队超限（背压拒绝）
    WAIT_TIMEOUT = "wait_timeout"              # 排队等待超时
    SHUTTING_DOWN = "shutting_down"            # 停机中，拒绝新请求
    NOT_READY = "not_ready"                    # 未就绪（预热未完成）
    BACKEND_ERROR = "backend_error"            # 后端异常且重试耗尽
    FALLBACK_UNAVAILABLE = "fallback_unavailable"  # 降级目标不可用
    CANCELLED = "cancelled"                    # 批次内请求被取消


class GatewayError(Exception):
    """携带原因码的网关异常。"""

    def __init__(self, reason: Reason, message: str = "") -> None:
        self.reason = reason
        super().__init__(message or reason.value)
