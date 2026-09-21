"""推理服务层：请求去重、结果缓存、重试与降级编排。

关键语义：
- 去重键 = request_id：相同标识的并发/紧接重复提交共享同一次执行，
  只产生一次真实推理，只计一次用量；指纹不一致则拒绝（原因可区分）。
- 缓存键 = (model, version, canonical_input)：版本变化天然不命中旧缓存。
- 重试仅针对后端推理失败，不重复计入成功用量；失败尝试也只算一次请求。
- 降级只跳一次：目标版本必须存在、未下线且后端就绪，否则明确失败，
  绝不返回过期/伪造结果。
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections import OrderedDict

from app.errors import (
    DuplicateRequestConflictError,
    FallbackUnavailableError,
    GatewayError,
    InferenceFailedError,
)
from app.models import InferResponse

# 近期完成请求的去重记录上限（FIFO，超出淘汰最旧），防止内存无界增长。
_DEDUP_HISTORY = 4096


def canonicalize(payload: dict) -> str:
    """规范化输入：递归排序键，保证等价输入得到相同缓存键。"""
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, default=str,
    )


class InferenceService:
    def __init__(self, config, registry, batcher, cache, stats, logger) -> None:
        self.config = config
        self.registry = registry
        self.batcher = batcher
        self.cache = cache
        self.stats = stats
        self.log = logger
        # request_id -> (fingerprint, future)，在途执行
        self._inflight: dict[str, tuple[str, asyncio.Future]] = {}
        # request_id -> (fingerprint, result_dict)，近期完成
        self._completed: "OrderedDict[str, tuple[str, dict]]" = OrderedDict()
        self._dedup_lock = asyncio.Lock()

    async def infer(
        self,
        request_id: str | None,
        model: str,
        version_or_alias: str | None,
        payload: dict,
    ) -> InferResponse:
        request_id = request_id or f"auto-{uuid.uuid4().hex[:12]}"
        # 指纹覆盖模型、版本/别名与输入：同 ID 但目标不同即视为冲突。
        fingerprint = canonicalize(
            {"model": model, "version": version_or_alias, "input": payload}
        )

        # 原子地“检查并登记”请求标识，杜绝并发重复执行。
        async with self._dedup_lock:
            if request_id in self._inflight:
                prior_fp, fut = self._inflight[request_id]
                if prior_fp != fingerprint:
                    raise DuplicateRequestConflictError(
                        f"duplicate request_id {request_id} with different payload"
                    )
                duplicate = True
            elif request_id in self._completed:
                prior_fp, result = self._completed[request_id]
                if prior_fp != fingerprint:
                    raise DuplicateRequestConflictError(
                        f"duplicate request_id {request_id} with different payload"
                    )
                duplicate = True
                fut = None
                result_dict = result
            else:
                duplicate = False
                loop = asyncio.get_running_loop()
                fut = loop.create_future()
                self._inflight[request_id] = (fingerprint, fut)

        if duplicate:
            if fut is not None:
                self.log.info(
                    "dedup request_id=%s reason=inflight_shared", request_id
                )
                result_dict = await fut
            else:
                self.log.info(
                    "dedup request_id=%s reason=recent_completed", request_id
                )
            # 仅累加“去重命中”观测计数；用量（received/succeeded/真实推理）
            # 仍只由主执行者计一次。
            dedup_key = f"{result_dict['model']}@{result_dict['version']}"
            self.stats.record_dedup_hit(dedup_key)
            return InferResponse(
                **{**result_dict, "request_id": request_id,
                   "deduplicated": True}
            )

        # 主执行者路径。
        try:
            resp = await self._execute(
                request_id, model, version_or_alias, payload, fingerprint, fut
            )
        except BaseException as exc:
            async with self._dedup_lock:
                self._inflight.pop(request_id, None)
                if not fut.done():
                    fut.set_exception(exc)
            raise
        # 成功后保留在近期完成表（有界），供紧接的重复提交去重。
        async with self._dedup_lock:
            self._inflight.pop(request_id, None)
            result_dict = resp.model_dump()
            if not fut.done():
                fut.set_result(result_dict)
            self._completed[request_id] = (fingerprint, result_dict)
            self._completed.move_to_end(request_id)
            while len(self._completed) > _DEDUP_HISTORY:
                self._completed.popitem(last=False)
        return resp

    async def _execute(
        self, request_id, model, version_or_alias, payload, fingerprint, fut
    ) -> InferResponse:
        # 路由（未知模型/版本/下线在此区分拒绝）。
        version = await self.registry.resolve(model, version_or_alias)
        key = f"{model}@{version}"
        # 用量只由主执行者登记一次；重复提交不再计数。
        self.stats.record_received(key)
        self.log.info(
            "route request_id=%s model=%s requested=%s resolved=%s "
            "reason=alias_or_version",
            request_id, model, version_or_alias, version,
        )

        # 缓存/合批使用的规范化输入只取决于 payload 本身。
        canonical = canonicalize(payload)
        try:
            result = await self._attempt(
                request_id, model, version, canonical, primary=True
            )
        except GatewayError as exc:
            self.stats.record_failure(key)
            self.log.info(
                "request failed request_id=%s key=%s reason=%s",
                request_id, key, getattr(exc, "reason", "error"),
            )
            raise

        self.stats.record_success(
            key, cached=result["cached"], dedup=False
        )
        return InferResponse(**result)

    async def _attempt(
        self, request_id, model, version, canonical, *, primary: bool,
        fallback_used: str | None = None, attempts_offset: int = 0,
    ) -> dict:
        """对指定版本执行：缓存 -> 合批，失败时按重试/降级策略处理。"""
        key = f"{model}@{version}"
        cache_key = (model, version, canonical)

        if self.config.cache.enabled:
            hit = self.cache.get(cache_key)
            if hit is not None:
                self.log.info(
                    "cache hit request_id=%s key=%s reason=version_input_match",
                    request_id, key,
                )
                return {
                    "request_id": request_id, "model": model, "version": version,
                    "output": hit, "cached": True, "deduplicated": False,
                    "batch_id": None, "fallback": fallback_used,
                    "attempts": attempts_offset,
                }

        max_retries = self.config.retry.max_retries if primary else 0
        attempts = attempts_offset
        last_error: BaseException | None = None
        # 首次 + 重试；重试不重复计成功用量。
        for attempt_no in range(max_retries + 1):
            attempts += 1
            if attempt_no > 0:
                self.stats.record_retry(key)
                self.log.info(
                    "retry request_id=%s key=%s attempt=%d "
                    "reason=inference_failed",
                    request_id, key, attempt_no + 1,
                )
            try:
                output, batch_id = await self.batcher.submit(
                    model, version, request_id, canonical
                )
            except InferenceFailedError as exc:
                last_error = exc
                continue
            # 背压/超时/停机等错误不重试，直接上抛。
            if self.config.cache.enabled:
                self.cache.put(cache_key, output)
            return {
                "request_id": request_id, "model": model, "version": version,
                "output": output, "cached": False, "deduplicated": False,
                "batch_id": batch_id, "fallback": fallback_used,
                "attempts": attempts,
            }

        # 重试耗尽 -> 尝试降级（仅主版本允许一次降级）。
        if primary:
            target = self.registry.fallback_target(model, version)
            usable = target is not None and self._fallback_usable(model, target)
            if usable:
                self.stats.record_fallback(key)
                self.log.info(
                    "fallback request_id=%s from=%s to=%s "
                    "reason=primary_exhausted",
                    request_id, key, f"{model}@{target}",
                )
                return await self._attempt(
                    request_id, model, target, canonical,
                    primary=False, fallback_used=target,
                    attempts_offset=attempts,
                )
            self.log.info(
                "fallback denied request_id=%s key=%s target=%s reason=%s",
                request_id, key, target,
                "not_configured" if target is None else "target_unavailable",
            )
            raise FallbackUnavailableError(
                f"no usable fallback for {key} (target={target})"
            ) from last_error

        raise last_error  # type: ignore[misc]

    def _fallback_usable(self, model: str, version: str) -> bool:
        backend = self.registry.backends.get((model, version))
        if backend is None:
            return False
        if (model, version) in self.registry.offline:
            return False
        return backend.ready
