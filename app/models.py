"""HTTP 请求/响应模型。"""

from __future__ import annotations

from pydantic import BaseModel, Field


class InferRequest(BaseModel):
    model: str = Field(..., description="模型名")
    version: str | None = Field(None, description="版本号或别名；缺省走默认别名")
    input: dict = Field(default_factory=dict)
    request_id: str | None = None


class InferResponse(BaseModel):
    request_id: str
    model: str
    version: str
    output: str
    cached: bool = False
    deduplicated: bool = False
    batch_id: int | None = None
    fallback: str | None = None
    attempts: int = 1


class StatsResponse(BaseModel):
    received: int
    succeeded: int
    failed: int
    cache_hits: int
    dedup_hits: int
    retries: int
    fallbacks: int
    real_inferences: int
    per_version: dict[str, dict]


class HealthResponse(BaseModel):
    ready: bool
    shutting_down: bool
    backends: dict[str, bool]
