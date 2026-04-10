# import requests
from collections.abc import Awaitable

from rq import Worker

from src.utils.redis_client import get_redis_client
from src.work.work_queue import iq, lq

client = get_redis_client()


def check_redis_connection() -> Awaitable[bool] | bool:
    res = client.ping()
    return res


def check_worker_status() -> dict:
    workers = {}
    workers[iq.name] = Worker.all(queue=iq)
    workers[lq.name] = Worker.all(queue=lq)
    return workers
