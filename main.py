"""本地运行入口：uv run uvicorn main:app --reload"""
from gateway.app import create_app
from gateway.demo import demo_config

app = create_app(demo_config())
