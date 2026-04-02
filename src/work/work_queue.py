from loguru import logger
from redis.exceptions import ConnectionError as RedisConnectionError
from rq import Queue

from ..utils.redis_client import get_redis_client


def _make_queues() -> tuple[Queue, Queue]:
    """Initialise the RQ queues, raising on connection failure.

    Raises:
        RedisConnectionError: If the Redis server is unreachable.
    """
    try:
        client = get_redis_client()
        client.ping()  # fail fast — give a clear error instead of a silent hang
        layers_queue = Queue("layers", connection=client)
        id_queue = Queue("id", connection=client)
        return layers_queue, id_queue
    except RedisConnectionError as exc:
        logger.error("Failed to connect to Redis: %s", exc)
        raise


lq, iq = _make_queues()


def get_status(task_id: str) -> dict[str, str]:
    task = lq.fetch_job(task_id)
    if not task:
        task = iq.fetch_job(task_id)
    if not task:
        return {"status": "not found"}
    return {"status": task.get_status().name}
