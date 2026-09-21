"""共享测试夹具：内存 ASGI 客户端工厂。"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import httpx
import pytest

from app.app import create_app


@pytest.fixture
def client_factory():
    """用法：
    async with client_factory(config) as client: ...
    """

    @asynccontextmanager
    async def _factory(config):
        logging.disable(logging.CRITICAL)
        app = create_app(config)
        transport = httpx.ASGITransport(app=app)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=transport, base_url="http://gw"
            ) as client:
                yield client, app

    return _factory
