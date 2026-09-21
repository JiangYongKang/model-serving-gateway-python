"""FastAPI 应用工厂：生命周期、异常映射、管理接口。"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from app.batcher import Batcher
from app.cache import ResultCache
from app.config import GatewayConfig, default_config
from app.errors import GatewayError
from app.logging_setup import setup_logging
from app.models import HealthResponse, InferRequest, InferResponse, StatsResponse
from app.resources import ResourcePool
from app.service import InferenceService
from app.stats import Stats


class _State:
    """生命周期内共享组件。"""

    def __init__(self, config: GatewayConfig, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger
        self.registry = None
        self.resources = ResourcePool(config.resources)
        self.batcher = None
        self.cache = ResultCache(config.cache.capacity)
        self.stats = Stats()
        self.service: InferenceService | None = None
        self.ready = False
        self.shutting_down = False


def create_app(config: GatewayConfig | None = None) -> FastAPI:
    config = config or default_config()
    logger = setup_logging()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # 延迟导入以避免循环依赖
        from app.registry import Registry

        state = _State(config, logger)
        registry = Registry(config)
        state.registry = registry

        # 启动预热：就绪状态严格反映后端真实可用性。
        logger.info("warmup start reason=startup")
        await registry.warmup_all()
        state.batcher = Batcher(
            registry, state.resources, config.batch, state.stats, logger
        )
        await state.batcher.start()
        state.service = InferenceService(
            config, registry, state.batcher, state.cache, state.stats, logger
        )
        state.ready = True
        logger.info("warmup done ready=true backends=%d", len(registry.backends))
        app.state.gw = state

        try:
            yield
        finally:
            # 优雅停机：先标记拒收，再等待在途请求完成，超时则拒绝剩余。
            logger.info("shutdown begin reason=shutdown_signal")
            state.shutting_down = True
            state.ready = False
            loop = asyncio.get_running_loop()
            deadline = loop.time() + config.shutdown_timeout

            async def _drain():
                while True:
                    inflight = len(state.service._inflight)
                    util = state.resources.utilization()
                    active = util["global"]["active"]  # type: ignore[index]
                    if inflight == 0 and active == 0:
                        return
                    await asyncio.sleep(0.01)

            try:
                await asyncio.wait_for(_drain(), timeout=config.shutdown_timeout)
                logger.info("shutdown drained reason=inflight_zero")
            except TimeoutError:
                logger.warning(
                    "shutdown timeout remaining_inflight=%d reason=deadline",
                    len(state.service._inflight),
                )
            await state.batcher.stop()
            logger.info("shutdown complete")

    app = FastAPI(title="model-serving-gateway", lifespan=lifespan)

    @app.exception_handler(GatewayError)
    async def _gateway_error_handler(request, exc: GatewayError):  # type: ignore[no-untyped-def]
        logger.warning(
            "rejected path=%s reason=%s detail=%s",
            request.url.path, exc.reason, str(exc),
        )
        return JSONResponse(
            status_code=exc.http_status,
            content={
                "error": exc.reason,
                "detail": str(exc),
                "request_id": request.headers.get("x-request-id"),
            },
        )

    @app.post("/v1/infer", response_model=InferResponse)
    async def infer(req: InferRequest):
        st: _State = app.state.gw
        if st.shutting_down:
            from app.errors import GatewayShuttingDownError

            raise GatewayShuttingDownError("gateway is shutting down")
        return await st.service.infer(  # type: ignore[union-attr]
            req.request_id, req.model, req.version, req.input
        )

    @app.get("/v1/stats", response_model=StatsResponse)
    async def get_stats():
        st: _State = app.state.gw
        return st.stats.snapshot()

    @app.get("/health/ready", response_model=HealthResponse)
    async def ready_probe():
        st: _State = app.state.gw
        ready = st.ready and st.registry.all_ready() and not st.shutting_down
        return HealthResponse(
            ready=ready,
            shutting_down=st.shutting_down,
            backends=st.registry.ready_snapshot(),
        )

    @app.get("/health/live")
    async def live_probe():
        return {"alive": True}

    # ---- 管理接口：别名切换、下线/上线、缓存、队列观测 ----

    @app.post("/admin/models/{model}/aliases/{alias}")
    async def set_alias(model: str, alias: str, version: str):
        st: _State = app.state.gw
        await st.registry.set_alias(model, alias, version)  # type: ignore[union-attr]
        st.logger.info(
            "alias switch model=%s alias=%s -> %s reason=admin",
            model, alias, version,
        )
        return {"model": model, "alias": alias, "version": version}

    @app.post("/admin/models/{model}/versions/{version}/offline")
    async def set_offline(model: str, version: str, offline: bool = True):
        st: _State = app.state.gw
        st.registry.set_offline(model, version, offline)  # type: ignore[union-attr]
        return {"model": model, "version": version, "offline": offline}

    @app.post("/admin/cache/clear")
    async def clear_cache():
        st: _State = app.state.gw
        st.cache.clear()
        return {"cleared": True, "size": 0}

    @app.get("/admin/utilization")
    async def utilization():
        st: _State = app.state.gw
        return st.resources.utilization()

    return app
