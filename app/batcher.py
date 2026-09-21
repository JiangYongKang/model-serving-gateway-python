"""动态合批器。

- 每个 (model, version) 一条独立队列、一个常驻收集 worker：
  不同模型/版本绝不混入同一批；慢模型不会阻塞其他模型。
- 触发 flush 三条件（任一满足）：达到 max_size / 到达合批窗口 /
  队首请求等待达到 max_wait。
- worker 只负责“收集 + 派发”：flush 以独立任务执行，真正的并发度由
  ResourcePool 的两级信号量决定；慢批不会卡住后续批的收集与派发。
- 有界准入：每个键“在批项数 + 排队项数”达到
  (每模型并发 * 批大小 + 排队上限) 即 QueueFullError，杜绝无界积压。
- 批内逐项结算 future：单项失败只 reject 自己，其他项正常返回。
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass


@dataclass
class BatchItem:
    request_id: str
    model: str
    version: str
    canonical_input: str
    future: "asyncio.Future[tuple[str, int]]"
    enqueued_at: float


class Batcher:
    def __init__(self, registry, resources, config, stats, logger) -> None:
        self.registry = registry
        self.resources = resources
        self.config = config  # BatchConfig
        self.stats = stats
        self.log = logger
        self._queues: dict[tuple[str, str], asyncio.Queue[BatchItem]] = {}
        self._workers: dict[tuple[str, str], asyncio.Task] = {}
        self._flush_tasks: set[asyncio.Task] = set()
        self._batch_seq = 0
        self._stopping = False
        # 每个键已派发、尚未完成的项数（占用资源或在资源层排队）。
        self._inflight_items: dict[tuple[str, str], int] = {}
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        # 为注册表中每个版本启动常驻收集 worker（预热前由 app 调用）。
        for key in self.registry.backends:
            await self._ensure_worker(key)

    async def stop(self) -> None:
        """优雅停机：停止接收；队列内未派发项明确拒绝；等待在途批完成。"""
        from app.errors import GatewayShuttingDownError

        self._stopping = True
        # 排空所有尚未派发的排队项（带明确原因）。
        for key, queue in list(self._queues.items()):
            while not queue.empty():
                item = queue.get_nowait()
                if not item.future.done():
                    item.future.set_exception(
                        GatewayShuttingDownError(
                            f"rejected during shutdown: {item.request_id}"
                        )
                    )
        # 让在途批执行完（它们的 future 有 max_wait 兜底，不会无限挂起）。
        if self._flush_tasks:
            await asyncio.gather(*self._flush_tasks, return_exceptions=True)
        for task in self._workers.values():
            task.cancel()
        for task in self._workers.values():
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _ensure_worker(self, key: tuple[str, str]) -> asyncio.Queue:
        queue = self._queues.get(key)
        if queue is None:
            queue = asyncio.Queue()
            self._queues[key] = queue
            self._workers[key] = asyncio.create_task(self._worker(key, queue))
        return queue

    def _capacity(self, key: tuple[str, str]) -> int:
        rc = self.resources.config
        # 可在资源层并发执行的项 + 允许额外排队的项。
        return rc.per_model_concurrency * self.config.max_size + rc.per_model_queue

    async def submit(
        self, model: str, version: str, request_id: str, canonical_input: str
    ) -> tuple[str, int]:
        """入队并等待批处理，返回 (output, batch_id)。"""
        from app.errors import (
            GatewayShuttingDownError,
            QueueFullError,
            WaitTimeoutError,
        )

        key = (model, version)
        if self._stopping:
            raise GatewayShuttingDownError("gateway is shutting down")
        queue = await self._ensure_worker(key)

        # 有界准入：在批 + 排队总量受限，超限立即拒绝。
        async with self._lock:
            inflight = self._inflight_items.get(key, 0)
            if inflight + queue.qsize() >= self._capacity(key):
                raise QueueFullError(
                    f"queue full for {model}@{version}: "
                    f"inflight={inflight} queued={queue.qsize()} "
                    f"cap={self._capacity(key)}"
                )
            loop = asyncio.get_running_loop()
            future: asyncio.Future[tuple[str, int]] = loop.create_future()
            item = BatchItem(
                request_id=request_id,
                model=model,
                version=version,
                canonical_input=canonical_input,
                future=future,
                enqueued_at=loop.time(),
            )
            await queue.put(item)
        self.log.info(
            "enqueue request_id=%s key=%s qsize=%d reason=queued_for_batch",
            request_id, f"{model}@{version}", queue.qsize(),
        )

        try:
            return await asyncio.wait_for(future, timeout=self.config.max_wait)
        except TimeoutError as e:
            if not future.done():
                future.cancel()
            raise WaitTimeoutError(
                f"batch wait timeout for {request_id}"
            ) from e

    async def _worker(self, key: tuple[str, str], queue: asyncio.Queue) -> None:
        model, version = key
        try:
            while True:
                items: list[BatchItem] = []
                try:
                    first = await asyncio.wait_for(
                        queue.get(), timeout=self.config.window
                    )
                except TimeoutError:
                    if self._stopping:
                        return
                    continue
                items.append(first)
                window = self.config.window
                deadline = asyncio.get_running_loop().time() + window

                while len(items) < self.config.max_size:
                    head_wait = (
                        asyncio.get_running_loop().time() - items[0].enqueued_at
                    )
                    if head_wait >= self.config.max_wait:
                        break
                    remaining = max(
                        0.0, deadline - asyncio.get_running_loop().time()
                    )
                    try:
                        nxt = await asyncio.wait_for(queue.get(), timeout=remaining)
                    except TimeoutError:
                        break
                    items.append(nxt)

                pending = [it for it in items if not it.future.done()]
                if not pending:
                    continue
                self._batch_seq += 1
                batch_id = self._batch_seq
                reason = (
                    "full" if len(pending) >= self.config.max_size else "window"
                )
                self.log.info(
                    "batch flush batch_id=%d key=%s size=%d reason=%s",
                    batch_id, f"{model}@{version}", len(pending), reason,
                )
                await self._dispatch(key, pending, batch_id)
        except asyncio.CancelledError:
            raise

    async def _dispatch(
        self, key: tuple[str, str], items: list[BatchItem], batch_id: int
    ) -> None:
        """把一批的执行派发为独立任务；并发度由 ResourcePool 决定。"""
        async with self._lock:
            self._inflight_items[key] = (
                self._inflight_items.get(key, 0) + len(items)
            )

        async def _run():
            try:
                await self._flush(key, items, batch_id)
            finally:
                async with self._lock:
                    self._inflight_items[key] -= len(items)
                    self._flush_tasks.discard(task)

        task = asyncio.create_task(_run())
        self._flush_tasks.add(task)

    async def _flush(
        self, key: tuple[str, str], items: list[BatchItem], batch_id: int
    ) -> None:
        model, version = key
        backend = self.registry.get_backend(model, version)
        try:
            async with self.resources.acquire(model):
                payload = [(it.request_id, it.canonical_input) for it in items]
                t0 = time.perf_counter()
                results = await backend.infer(payload)
                dt = (time.perf_counter() - t0) * 1000
            self.stats.record_real(key, n=len(items))
            self.log.info(
                "batch done batch_id=%d key=%s size=%d elapsed_ms=%.1f "
                "reason=backend_returned",
                batch_id, f"{model}@{version}", len(items), dt,
            )
        except BaseException as exc:  # 整批获取资源失败（背压/停机）
            for it in items:
                if not it.future.done():
                    it.future.set_exception(exc)
            self.log.info(
                "batch rejected batch_id=%d key=%s size=%d reason=%s",
                batch_id, f"{model}@{version}", len(items),
                getattr(exc, "reason", "error"),
            )
            return

        # 逐项结算：单项失败不污染同批其他请求。
        for it, (out, err) in zip(items, results):
            if it.future.done():
                continue
            if err is not None:
                self.log.info(
                    "batch item failed batch_id=%d request_id=%s reason=%s",
                    batch_id, it.request_id, getattr(err, "reason", "error"),
                )
                it.future.set_exception(err)
            else:
                self.log.info(
                    "batch item ok batch_id=%d request_id=%s output=%s",
                    batch_id, it.request_id, out,
                )
                it.future.set_result((out, batch_id))
