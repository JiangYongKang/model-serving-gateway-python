"""FastAPI 入口：HTTP 接口暴露网关能力。

- POST /infer          推理（model, prompt, version?, request_id?）
- GET  /ready          就绪状态（反映后端真实可用性）
- GET  /stats          整体用量统计
- GET  /stats/versions 分版本观测统计
- POST /admin/alias    切换别名（单版本，等价权重 1）
- POST /admin/weights  灰度放量：别名 -> 多版本权重
- POST /admin/rollback 一键回退：仅把目标别名收敛为单版本（不涉及其它别名/降级/版本下线）
- POST /admin/breaker  配置版本自动止损（失败比例 + 观察窗口）
- POST /admin/retire   下线版本
错误响应统一为 {"detail": {"reason": <原因码>, "message": <说明>}}。
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from gateway.config import BreakerConfig, GatewayConfig
from gateway.errors import GatewayError, Reason
from gateway.gateway import Gateway

_STATUS_BY_REASON = {
    Reason.UNKNOWN_MODEL: 404,
    Reason.UNKNOWN_VERSION: 404,
    Reason.UNKNOWN_ALIAS: 404,
    Reason.VERSION_RETIRED: 410,
    Reason.VERSION_TRIPPED: 503,
    Reason.QUEUE_FULL: 429,
    Reason.WAIT_TIMEOUT: 429,
    Reason.NOT_READY: 503,
    Reason.SHUTTING_DOWN: 503,
    Reason.BACKEND_ERROR: 502,
    Reason.FALLBACK_UNAVAILABLE: 502,
    Reason.CANCELLED: 500,
}


class InferRequest(BaseModel):
    model: str
    prompt: str
    version: str | None = None
    request_id: str | None = None


class AliasRequest(BaseModel):
    model: str
    alias: str
    version: str


class WeightsRequest(BaseModel):
    model: str
    alias: str
    weights: dict[str, float]


class RollbackRequest(BaseModel):
    model: str
    alias: str
    version: str


class BreakerRequest(BaseModel):
    model: str
    version: str
    failure_ratio: float = 0.5
    window: int = 20


class RetireRequest(BaseModel):
    model: str
    version: str


def create_app(config: GatewayConfig | None = None) -> FastAPI:
    gateway = Gateway(config or GatewayConfig())

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await gateway.start()
        yield
        await gateway.shutdown()

    app = FastAPI(title="model-serving-gateway", lifespan=lifespan)
    app.state.gateway = gateway

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse(
            status_code=_STATUS_BY_REASON.get(exc.reason, 500),
            content={"detail": {"reason": exc.reason.value, "message": str(exc)}},
        )

    @app.post("/infer")
    async def infer(req: InferRequest) -> dict[str, str]:
        result = await gateway.infer(req.model, req.prompt, req.version, req.request_id)
        return {"result": result}

    @app.get("/ready")
    async def ready() -> JSONResponse:
        return JSONResponse(
            status_code=200 if gateway.ready else 503,
            content={"ready": gateway.ready},
        )

    @app.get("/stats")
    async def stats() -> dict[str, int]:
        return gateway.stats.snapshot()

    @app.get("/stats/versions")
    async def version_stats() -> dict[str, dict[str, int]]:
        return gateway.version_stats_snapshot()

    @app.post("/admin/alias")
    async def set_alias(req: AliasRequest) -> dict[str, str]:
        gateway.registry.set_alias(req.model, req.alias, req.version)
        return {"status": "ok"}

    @app.post("/admin/weights")
    async def set_weights(req: WeightsRequest) -> dict[str, str]:
        gateway.registry.set_alias_weights(req.model, req.alias, req.weights)
        return {"status": "ok"}

    @app.post("/admin/rollback")
    async def rollback(req: RollbackRequest) -> dict[str, object]:
        revoked = await gateway.rollback(req.model, req.alias, req.version)
        return {"status": "ok", "revoked": revoked}

    @app.post("/admin/breaker")
    async def set_breaker(req: BreakerRequest) -> dict[str, str]:
        gateway.set_breaker(
            req.model, req.version, BreakerConfig(req.failure_ratio, req.window)
        )
        return {"status": "ok"}

    @app.post("/admin/retire")
    async def retire(req: RetireRequest) -> dict[str, str]:
        gateway.registry.retire_version(req.model, req.version)
        return {"status": "ok"}

    return app
