"""骨架可导入性冒烟测试。"""
import gateway  # noqa: F401
from gateway.app import create_app
from gateway.config import GatewayConfig
from gateway.gateway import Gateway


def test_import_and_construct():
    gw = Gateway(GatewayConfig())
    assert gw is not None
    app = create_app(GatewayConfig())
    assert app is not None
