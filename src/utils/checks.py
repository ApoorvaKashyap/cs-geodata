# import requests
"""Health check API endpoints and diagnostics."""

from collections.abc import Awaitable

from rq import Worker

from src.utils.redis_client import get_redis_client
from src.work.work_queue import bq, lq, mq

client = get_redis_client()


def check_redis_connection() -> Awaitable[bool] | bool:
    """Check if the Redis connection is alive.

    Returns:
        True if the connection is active, False otherwise.

    """
    res = client.ping()
    return res


def check_worker_status() -> dict:
    """Check the status of all active RQ workers.

    Returns:
        A dictionary containing worker metrics.

    """
    workers = {}
    workers[lq.name] = Worker.all(queue=lq)
    workers[bq.name] = Worker.all(queue=bq)
    workers[mq.name] = Worker.all(queue=mq)
    return workers
