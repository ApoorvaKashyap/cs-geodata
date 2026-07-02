from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import FastAPI
from loguru import logger

from src.routers.geojson import router as geojson_router
from src.utils.checks import check_redis_connection, check_worker_status
from src.work.work_queue import get_status


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, Any]:
    # Startup tasks
    logger.add("logs/converter.logs", retention="10 days")
    logger.info("Application Startup Completed!")
    yield
    # Shutdown tasks


app = FastAPI(lifespan=lifespan)


@app.get(path="/")
async def read_root() -> dict[str, str]:
    if not check_redis_connection():
        return {
            "status": "error",
            "message": "Redis connection or worker status check failed",
        }
    workers = check_worker_status()

    missing_queues = [q for q, w in workers.items() if len(w) == 0]
    if missing_queues:
        return {
            "status": "error",
            "message": f"No workers running for queues: {', '.join(missing_queues)}",
        }

    worker_counts = " ".join(
        [f"{q.capitalize()} workers: {len(w)}" for q, w in workers.items()]
    )
    return {
        "status": "ok",
        "message": f"All systems connected! {worker_counts}",
    }


@app.get(path="/api/v1/status")
async def get_jobstatus(task_id: str) -> dict[str, str]:
    return await get_status(task_id)


app.include_router(geojson_router, prefix="/api/v1")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0")
