from rq import Queue

from src.utils.configs import get_settings
from src.utils.redis_client import get_redis_client


def get_runs_queue() -> Queue:
    """Return the RQ queue for Descriptor orchestration jobs.

    Returns:
        Queue that receives one job per Descriptor per run.
    """
    return Queue(get_settings().rq_runs_queue, connection=get_redis_client())


def get_fetch_queue() -> Queue:
    """Return the RQ queue for WFS fetch jobs.

    Returns:
        Queue that receives one job per tehsil per WFS layer.
    """
    return Queue(get_settings().rq_fetch_queue, connection=get_redis_client())


def get_pipeline_queues() -> tuple[Queue, Queue]:
    """Return the configured pipeline RQ queues.

    Returns:
        Runs queue and fetch queue.
    """
    return get_runs_queue(), get_fetch_queue()
