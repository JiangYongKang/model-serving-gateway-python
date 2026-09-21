"""可区分原因的错误类型。"""


class GatewayError(Exception):
    """网关错误基类。"""

    reason: str = "gateway_error"
    http_status: int = 500


class UnknownModelError(GatewayError):
    reason = "unknown_model"
    http_status = 404


class UnknownVersionError(GatewayError):
    reason = "unknown_version"
    http_status = 404


class ModelOfflineError(GatewayError):
    reason = "model_offline"
    http_status = 410


class QueueFullError(GatewayError):
    reason = "queue_full"
    http_status = 429


class WaitTimeoutError(GatewayError):
    reason = "wait_timeout"
    http_status = 408


class InferenceFailedError(GatewayError):
    reason = "inference_failed"
    http_status = 502


class FallbackUnavailableError(GatewayError):
    reason = "fallback_unavailable"
    http_status = 503


class GatewayShuttingDownError(GatewayError):
    reason = "shutting_down"
    http_status = 503


class DuplicateRequestConflictError(GatewayError):
    reason = "duplicate_id_conflict"
    http_status = 409
