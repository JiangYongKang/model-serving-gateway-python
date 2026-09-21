"""FastAPI 入口：HTTP 接口暴露网关能力。"""
from __future__ import annotations

from fastapi import FastAPI

from gateway.config import GatewayConfig
from gateway.gateway import Gateway


def create_app(config: GatewayConfig | None = None) -> FastAPI:
    app = FastAPI(title="model-serving-gateway")
    app.state.gateway = Gateway(config or GatewayConfig())
    return app
