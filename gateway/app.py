"""FastAPI 入口：HTTP 接口暴露网关能力。

- POST /infer        推理（model, prompt, version?, request_id?）
- GET  /ready        就绪状态（反映后端真实可用性）
- GET  /stats        用量统计
- POST /admin/alias  切换别名
- POST /admin/retire 下线版本
错误响应统一为 {"detail": {"reason": <原因码>, "message": <说明>}}。
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from gateway.config import GatewayConfig
from gateway.errors import GatewayError, Reason
from gateway.gateway import Gateway

_STATUS_BY_REASON = {
    Reason.UNKNOWN_MODEL: 404,
    Reason.UNKNOWN_VERSION: 404,
    Reason.UNKNOWN_ALIAS: 404,
    Reason.VERSION_RETIRED: 410,
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

    @app.post("/admin/alias")
    async def set_alias(req: AliasRequest) -> dict[str, str]:
        gateway.registry.set_alias(req.model, req.alias, req.version)
        return {"status": "ok"}

    @app.post("/admin/retire")
    async def retire(req: RetireRequest) -> dict[str, str]:
        gateway.registry.retire_version(req.model, req.version)
        return {"status": "ok"}

    return app
