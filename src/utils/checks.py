from rq import Worker
from rq.worker import BaseWorker

from src.utils.configs import get_settings
from src.utils.redis_client import get_redis_client


def check_redis_connection() -> bool:
    """Return whether Redis responds to a ping.

    Returns:
        True when Redis is reachable.
    """
    return bool(get_redis_client().ping())


def check_worker_status() -> dict[str, list[BaseWorker]]:
    """Return active workers for configured pipeline queue names.

    Returns:
        Mapping from queue name to active workers.
    """
    settings = get_settings()
    workers = Worker.all(connection=get_redis_client())
    return {
        settings.rq_runs_queue: [
            worker
            for worker in workers
            if settings.rq_runs_queue in worker.queue_names()
        ],
        settings.rq_fetch_queue: [
            worker
            for worker in workers
            if settings.rq_fetch_queue in worker.queue_names()
        ],
    }
