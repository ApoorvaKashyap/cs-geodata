"""RQ worker entrypoint for the GeoData pipeline."""

from __future__ import annotations

from rq import Worker

from src.utils.configs import get_settings
from src.utils.redis_client import get_redis_client


def create_worker() -> Worker:
    """Create an RQ worker for the configured pipeline queues.

    Returns:
        Worker subscribed to the runs and fetch queues.
    """
    settings = get_settings()
    return Worker(
        [settings.rq_runs_queue, settings.rq_fetch_queue],
        connection=get_redis_client(),
    )


if __name__ == "__main__":
    create_worker().work()
