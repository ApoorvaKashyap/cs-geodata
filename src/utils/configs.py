from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration read from environment variables or a .env file.

    Attributes:
        redis_url: Full Redis connection URL, e.g. ``redis://localhost:6379/0``.
        redis_progress_ttl_seconds: Retention period for run-progress keys in Redis.
        rq_runs_queue: RQ queue that receives one job per Descriptor per run.
        rq_fetch_queue: RQ queue that receives one job per tehsil per WFS layer.
        rq_job_timeout: Maximum wall-clock seconds an RQ job may run before being killed.
        s3_endpoint_url: Optional custom S3 endpoint for MinIO or localstack.
        aws_access_key_id: AWS credential for S3 access.
        aws_secret_access_key: AWS credential for S3 access.
        aws_region: AWS region for S3 operations.
        s3_descriptor_bucket: Default bucket for Descriptor TOMLs when not in the URI.
        geoserver_base_url: Base URL of the CoreStack GeoServer instance, no trailing slash.
        wfs_max_concurrent_fetches: Max parallel tehsil requests issued by WFSSource.
        wfs_request_timeout_seconds: Per-request timeout for WFS GeoJSON fetches.
        wfs_version: WFS protocol version to request from GeoServer.
        stac_request_timeout_seconds: Timeout for STAC item HTTP fetches during validation.
        duckdb_memory_limit: Memory limit passed to DuckDB at connection time.
        duckdb_threads: Number of threads DuckDB may use during write operations.
        run_workspace_root: Parent directory for UUID-scoped per-run working directories.
        log_level: Standard Python logging level name.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    redis_url: str | None = None
    redis_progress_ttl_seconds: int = 60 * 60 * 24 * 7

    rq_runs_queue: str = "runs"
    rq_fetch_queue: str = "fetch"
    rq_job_timeout: int = 60 * 60 * 6

    s3_endpoint_url: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_region: str = "ap-south-1"
    s3_descriptor_bucket: str | None = None

    geoserver_base_url: str | None = None
    wfs_max_concurrent_fetches: int = 20
    wfs_request_timeout_seconds: int = 120
    wfs_version: str = "2.0.0"

    stac_request_timeout_seconds: int = 30

    duckdb_memory_limit: str = "4GB"
    duckdb_threads: int = 4

    run_workspace_root: Path = Path("/tmp/runs")
    log_level: str = "INFO"

    @field_validator("run_workspace_root", mode="before")
    @classmethod
    def _coerce_path(cls, v: object) -> Path:
        return Path(v)  # type: ignore[arg-type]

    @model_validator(mode="after")
    def _require_critical_fields(self) -> Settings:
        missing = [
            name
            for name, value in [
                ("REDIS_URL", self.redis_url),
                ("GEOSERVER_BASE_URL", self.geoserver_base_url),
            ]
            if not value
        ]
        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}. "
                "Set them in your shell or in a .env file."
            )
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton Settings instance.

    Reads the .env file and environment variables exactly once per process.
    Import and call this function everywhere instead of constructing
    ``Settings()`` directly.

    Returns:
        The shared Settings instance.

    Raises:
        ValidationError: If REDIS_URL or GEOSERVER_BASE_URL are not set.
    """
    return Settings()
