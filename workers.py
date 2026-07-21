from rq import Worker

from src.utils.redis_client import get_redis_client

from loguru import logger

logger.add("logs/workers.logs")
logger.info("Worker started")

w = Worker(["layers", "base", "meta"], connection=get_redis_client())

if __name__ == "__main__":
    w.work()
