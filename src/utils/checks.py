# import requests
from collections.abc import Awaitable

from rq import Worker

from src.utils.redis_client import get_redis_client
from src.work.work_queue import bq, lq, mq

client = get_redis_client()


def check_redis_connection() -> Awaitable[bool] | bool:
    res = client.ping()
    return res


def check_worker_status() -> dict:
    workers = {}
    workers[lq.name] = Worker.all(queue=lq)
    workers[bq.name] = Worker.all(queue=bq)
    workers[mq.name] = Worker.all(queue=mq)
    return workers
