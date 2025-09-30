from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)


@dataclass
class Job:
    func: Callable[..., Any]
    args: Sequence[Any]
    kwargs: Dict[str, Any]
    future: asyncio.Future


class TaskQueue:
    def __init__(self, *, maxsize: int, workers: int, retries: int, backoff: float) -> None:
        self._maxsize = maxsize
        self._worker_count = max(workers, 1)
        self._retries = max(retries, 1)
        self._backoff = backoff

        self._queue: Optional[asyncio.Queue[Job]] = None
        self._workers: List[asyncio.Task] = []

    @property
    def max_size(self) -> int:
        return self._maxsize

    @property
    def worker_count(self) -> int:
        return self._worker_count

    async def submit(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        if self._queue is None:
            raise RuntimeError("Task queue is not running")

        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        job = Job(func=func, args=args, kwargs=kwargs, future=future)
        await self._queue.put(job)
        return await future

    async def start(self) -> None:
        if self._queue is not None:
            return
        self._queue = asyncio.Queue(maxsize=self._maxsize)
        self._workers = [
            asyncio.create_task(self._worker_loop(index), name=f"lamapi-worker-{index}")
            for index in range(self._worker_count)
        ]
        logger.info("Started %s task queue workers", self._worker_count)

    async def stop(self) -> None:
        if self._queue is None:
            return
        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(*self._workers, return_exceptions=True)

        while not self._queue.empty():
            job = self._queue.get_nowait()
            if not job.future.done():
                job.future.set_exception(RuntimeError("Server shutting down"))
            self._queue.task_done()

        self._queue = None
        self._workers = []
        logger.info("Task queue stopped")

    async def _worker_loop(self, worker_id: int) -> None:
        assert self._queue is not None
        queue = self._queue
        loop = asyncio.get_running_loop()
        while True:
            try:
                job = await queue.get()
            except asyncio.CancelledError:
                break

            try:
                last_exc: Optional[Exception] = None
                for attempt in range(self._retries):
                    try:
                        call = partial(job.func, *job.args, **job.kwargs)
                        result = await loop.run_in_executor(None, call)
                        if not job.future.done():
                            job.future.set_result(result)
                        break
                    except Exception as exc:  # noqa: BLE001
                        last_exc = exc
                        if attempt < self._retries - 1:
                            await asyncio.sleep(self._backoff * (attempt + 1))
                        else:
                            if not job.future.done():
                                job.future.set_exception(exc)
                else:
                    if last_exc and not job.future.done():
                        job.future.set_exception(last_exc)
            except Exception as exc:  # noqa: BLE001
                logger.exception("Worker %s execution failure", worker_id)
                if not job.future.done():
                    job.future.set_exception(exc)
            finally:
                queue.task_done()
