"""FastAPI entrypoint for the GeoData pipeline."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import uvicorn
from fastapi import FastAPI
from loguru import logger
from redis.exceptions import RedisError

from src.api.routes import router as pipeline_router
from src.utils.configs import get_settings
from src.utils.redis_client import get_redis_client


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, Any]:
    """Configure process-level application resources.

    Args:
        app: FastAPI application instance.

    Yields:
        Control back to FastAPI while the application is running.
    """
    settings = get_settings()
    logger.remove()
    logger.add("logs/converter.logs", retention="10 days", level=settings.log_level)
    logger.add(lambda message: print(message, end=""), level=settings.log_level)
    logger.info("Application startup completed.")
    yield
    logger.info("Application shutdown completed.")


app = FastAPI(
    title="GeoData Pipeline",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/")
async def health() -> dict[str, str]:
    """Return basic API health.

    Returns:
        Health status payload.
    """
    return {"status": "ok"}


@app.get("/health/ready")
async def readiness() -> dict[str, str]:
    """Return dependency readiness for accepting pipeline work.

    Returns:
        Readiness payload with Redis status.
    """
    try:
        get_redis_client().ping()
    except RedisError as exc:
        return {"status": "error", "redis": str(exc)}
    return {"status": "ok", "redis": "ok"}


app.include_router(pipeline_router)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0")
