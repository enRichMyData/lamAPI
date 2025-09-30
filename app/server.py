import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import api_router
from app.core.config import get_settings
from app.dependencies import get_task_queue, shutdown_task_queue, startup_task_queue

settings = get_settings()

logger = logging.getLogger("lamapi.server")


@asynccontextmanager
async def lifespan(_: FastAPI):
    await startup_task_queue()
    queue = get_task_queue()
    logger.info(
        "LamAPI FastAPI server started with queue size %s and worker count %s",
        queue.max_size,
        queue.worker_count,
    )
    try:
        yield
    finally:
        await shutdown_task_queue()


app = FastAPI(
    title=settings.app_name,
    description=settings.description,
    version=settings.app_version,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)
