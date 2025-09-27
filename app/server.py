import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import api_router
from app.core.config import get_settings
from app.dependencies import get_task_queue, shutdown_task_queue, startup_task_queue

settings = get_settings()

logger = logging.getLogger("lamapi.server")

app = FastAPI(
    title=settings.app_name, description=settings.description, version=settings.app_version
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def on_startup() -> None:
    await startup_task_queue()
    task_queue = get_task_queue()
    logger.info(
        "LamAPI FastAPI server started with queue size %s and worker count %s",
        task_queue.max_size,
        task_queue.worker_count,
    )


@app.on_event("shutdown")
async def on_shutdown() -> None:
    await shutdown_task_queue()


app.include_router(api_router)
