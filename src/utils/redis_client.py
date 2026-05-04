"""Shared Redis client factory."""

from redis import Redis

from src.utils.configs import get_settings

_client: Redis | None = None


def get_redis_client() -> Redis:
    """Return the process-wide Redis client, creating it on first call.

    The client is lazily initialised and cached for the lifetime of the process.
    It reads the Redis URL from :mod:`src.utils.configs`.

    Returns:
        A Redis client configured from ``REDIS_URL``.

    Raises:
        pydantic.ValidationError: If required settings are missing or invalid.
    """
    global _client
    if _client is None:
        settings = get_settings()
        _client = Redis.from_url(settings.redis_url, socket_connect_timeout=3)
    return _client
