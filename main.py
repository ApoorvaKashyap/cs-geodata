from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import FastAPI
from loguru import logger

from src.utils.checks import check_redis_connection, check_worker_status
from src.work.work_queue import get_status

app = FastAPI()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, Any]:
    # Startup tasks
    logger.add("/var/logs/converter.logs")
    yield
    # Shutdown tasks


@app.get(path="/")
async def read_root() -> dict[str, str]:
    if not check_redis_connection():
        return {
            "status": "error",
            "message": "Redis connection or worker status check failed",
        }
    workers = check_worker_status()
    if len(workers["id"]) == 0:
        return {
            "status": "error",
            "message": "No ID workers running",
        }
    if len(workers["layers"]) == 0:
        return {
            "status": "error",
            "message": "No layers workers running",
        }
    return {
        "status": "ok",
        "message": f"All systems connected! ID workers: {len(workers['id'])} Layers workers: {len(workers['layers'])}",
    }


@app.get(path="/api/v1/status")
async def get_jobstatus(task_id: str) -> dict[str, str]:
    return get_status(task_id)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0")
