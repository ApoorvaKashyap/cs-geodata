"""Shared Redis client factory.

A single :func:`get_redis_client` call is the canonical way to obtain a
``Redis`` connection anywhere in the project.  All callers share the same
configuration source (:mod:`src.utils.configs`) so the host/port only ever
needs to change in one place (``.env`` or environment variables).
"""

from redis import Redis

from src.utils.configs import settings

_client: Redis | None = None


def get_redis_client() -> Redis:
    """Return the process-wide Redis client, creating it on first call.

    The client is lazily initialised and cached for the lifetime of the
    process.  It reads ``redis_host`` and ``redis_port`` from
    :data:`~src.utils.configs.settings` (populated from ``.env`` or
    real environment variables).

    Returns:
        A connected :class:`redis.Redis` instance.

    Raises:
        redis.exceptions.ConnectionError: If the Redis server cannot be
            reached on the first ping.
    """
    global _client
    if _client is None:
        _client = Redis(
            host=settings.redis_host,
            port=settings.redis_port,
            socket_connect_timeout=3,
        )
    return _client
