from app.core.config import get_settings
from app.core.task_queue import TaskQueue
from lamapi import LamAPI

settings = get_settings()
lamapi_service = LamAPI()

queue = TaskQueue(
    maxsize=settings.queue_max_size,
    workers=settings.queue_workers,
    retries=settings.queue_retries,
    backoff=settings.queue_backoff,
)


def get_lamapi() -> LamAPI:
    return lamapi_service


def get_task_queue() -> TaskQueue:
    return queue


async def startup_task_queue() -> None:
    await queue.start()


async def shutdown_task_queue() -> None:
    await queue.stop()
