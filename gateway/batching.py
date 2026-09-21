"""动态合批器：按 (模型, 版本) 分桶，窗口/批大小/等待上限可配置。

- 不同 (model, version) 键的请求绝不混入同一批；
- 有效窗口 = min(window_ms, max_wait_ms)，保证单请求等待有上限；
- 批内单项失败以 ItemFailure 落在该项 future 上，不污染同批其他请求；
- 整体故障时该批所有请求收到同一 BackendError（由上层重试/降级）。
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from gateway.backends import BackendError, ItemFailure
from gateway.config import BatchConfig
from gateway.errors import GatewayError, Reason

InferFn = Callable[[list[str]], Awaitable[list[str | ItemFailure]]]


@dataclass
class _Bucket:
    infer: InferFn
    items: list[tuple[str, asyncio.Future]] = field(default_factory=list)
    timer: asyncio.Task | None = None


class BatchScheduler:
    def __init__(self, config: BatchConfig) -> None:
        self._cfg = config
        self._buckets: dict[tuple[str, str], _Bucket] = {}
        self._flush_tasks: set[asyncio.Task] = set()
        self._closing = False

    async def submit(self, key: tuple[str, str], prompt: str, infer: InferFn) -> str:
        if self._closing:
            raise GatewayError(Reason.SHUTTING_DOWN, "batch scheduler is closing")
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[str] = loop.create_future()
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = self._buckets[key] = _Bucket(infer=infer)
        bucket.items.append((prompt, fut))
        if len(bucket.items) >= self._cfg.max_batch_size:
            self._flush_now(key)
        elif bucket.timer is None:
            bucket.timer = loop.create_task(self._flush_later(key))
        return await fut

    def _effective_window_s(self) -> float:
        return min(self._cfg.window_ms, self._cfg.max_wait_ms) / 1000.0

    async def _flush_later(self, key: tuple[str, str]) -> None:
        try:
            await asyncio.sleep(self._effective_window_s())
            self._flush_now(key)
        except asyncio.CancelledError:
            pass

    def _flush_now(self, key: tuple[str, str]) -> None:
        bucket = self._buckets.pop(key, None)
        if bucket is None or not bucket.items:
            return
        if bucket.timer is not None and not bucket.timer.done():
            bucket.timer.cancel()
        items, bucket.items = bucket.items, []
        task = asyncio.get_running_loop().create_task(self._run_batch(key, bucket.infer, items))
        self._flush_tasks.add(task)
        task.add_done_callback(self._flush_tasks.discard)

    async def _run_batch(
        self,
        key: tuple[str, str],
        infer: InferFn,
        items: list[tuple[str, asyncio.Future]],
    ) -> None:
        prompts = [p for p, _ in items]
        try:
            results = await infer(prompts)
        except BackendError as exc:
            for _, fut in items:
                if not fut.done():
                    fut.set_exception(exc)
            return
        except Exception as exc:  # 未知异常也不允许泄漏到调度器
            for _, fut in items:
                if not fut.done():
                    fut.set_exception(BackendError(str(exc)))
            return
        for (prompt, fut), res in zip(items, results, strict=True):
            if fut.done():
                continue
            if isinstance(res, BackendError):
                fut.set_exception(res)
            else:
                fut.set_result(res)

    async def shutdown(self) -> None:
        """停止接收新请求，立即冲刷剩余桶并等待在途批次完成。"""
        self._closing = True
        for key in list(self._buckets):
            self._flush_now(key)
        for task in list(self._flush_tasks):
            await asyncio.gather(task, return_exceptions=True)
        for bucket in self._buckets.values():
            if bucket.timer is not None and not bucket.timer.done():
                bucket.timer.cancel()
