"""Utility for S3 storage options."""

from __future__ import annotations

from typing import Any

from src.utils.configs import get_settings


def get_s3_storage_options() -> dict[str, Any]:
    """Return fsspec storage options derived from application settings.

    Returns:
        Dictionary compatible with fsspec/s3fs storage_options.
    """
    settings = get_settings()
    options: dict[str, Any] = {
        "key": settings.aws_access_key_id,
        "secret": settings.aws_secret_access_key,
        "client_kwargs": {"region_name": settings.aws_region},
    }

    if settings.s3_endpoint_url:
        options["endpoint_url"] = settings.s3_endpoint_url

    # Remove None values
    return {k: v for k, v in options.items() if v is not None}
