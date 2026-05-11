"""Pipeline fetch stage — dispatches to the correct source and returns a LazyFrame."""

from __future__ import annotations

import time
from pathlib import Path

import polars as pl
from rq.job import Job, JobStatus

from src.descriptor.schema import LayerDescriptor
from src.pipeline.fetch_job import fetch_tehsil_job
from src.sources.s3 import S3Source
from src.sources.wfs import TehsilKey, walk_stac_tehsil_hierarchy
from src.stac.client import StacSourceInfo
from src.utils.redis_client import get_redis_client
from src.work.work_queue import get_fetch_queue

_POLL_INTERVAL_SECONDS = 0.5
_TERMINAL_STATUSES = {JobStatus.FINISHED, JobStatus.FAILED, JobStatus.STOPPED}


async def fetch_layer(
    layer: LayerDescriptor,
    source_info: StacSourceInfo,
    scratch_dir: Path,
) -> pl.LazyFrame:
    """Fetch raw data for one layer and return it as a LazyFrame.

    For S3 sources, downloads the file directly to the scratch directory.

    For WFS sources, dispatches one RQ job per tehsil onto the ``fetch`` queue.
    Each fetch worker downloads and writes one tehsil's data as a Parquet file
    to the shared workspace. The runs worker waits here (polling) until all jobs
    reach a terminal state, then reads and concatenates the results.

    RQ's built-in Redis tracking records per-job status automatically — no
    manual progress updates are required.

    Args:
        layer: Layer Descriptor being fetched.
        source_info: Resolved STAC source metadata for the layer.
        scratch_dir: Per-run scratch directory for downloads and intermediate
            Parquet files.

    Returns:
        Concatenated LazyFrame with raw source columns.

    Raises:
        RuntimeError: If the source type is unsupported or fetch jobs fail.
    """
    if source_info.source_type == "s3":
        source = S3Source(asset_href=source_info.asset_href, scratch_dir=scratch_dir)
        return await source.fetch()

    if source_info.source_type == "wfs":
        return await _fetch_wfs(layer, source_info, scratch_dir)

    raise RuntimeError(f"Unsupported source type: {source_info.source_type!r}")


async def _fetch_wfs(
    layer: LayerDescriptor,
    source_info: StacSourceInfo,
    scratch_dir: Path,
) -> pl.LazyFrame:
    if not layer.url_template:
        raise RuntimeError(
            f"Layer '{layer.name}' is a WFS source but has no url_template."
        )

    tehsil_keys: list[TehsilKey] = await walk_stac_tehsil_hierarchy(layer.stac_item)
    if not tehsil_keys:
        raise RuntimeError(
            f"STAC hierarchy for layer '{layer.name}' returned no tehsil keys."
        )

    fetch_queue = get_fetch_queue()
    redis = get_redis_client()
    layer_scratch = scratch_dir / layer.name
    layer_scratch.mkdir(parents=True, exist_ok=True)

    jobs: list[tuple[Job, Path]] = []
    for key in tehsil_keys:
        url = layer.url_template.format(
            state=key.state,
            district=key.district,
            tehsil=key.tehsil,
        )
        output_path = (
            layer_scratch / f"{key.state}__{key.district}__{key.tehsil}.parquet"
        )
        job = fetch_queue.enqueue(
            fetch_tehsil_job,
            str(url),
            str(output_path),
        )
        jobs.append((job, output_path))

    failed_job_ids = _wait_for_jobs(jobs, redis)
    if failed_job_ids:
        raise RuntimeError(
            f"Layer '{layer.name}': {len(failed_job_ids)} of {len(jobs)} "
            "tehsil fetch jobs failed."
        )

    frames = [
        pl.scan_parquet(path)
        for _, path in jobs
        if path.exists() and path.stat().st_size > 0
    ]
    if not frames:
        raise RuntimeError(f"Layer '{layer.name}': no tehsil data was fetched.")

    return pl.concat(frames, how="diagonal_relaxed")


def _wait_for_jobs(
    jobs: list[tuple[Job, Path]],
    redis: object,
) -> set[str]:
    pending = {job.id for job, _ in jobs}
    failed: set[str] = set()

    while pending:
        completed: set[str] = set()
        for job_id in pending:
            status = Job.fetch(job_id, connection=redis).get_status()
            if status not in _TERMINAL_STATUSES:
                continue

            completed.add(job_id)
            if status == JobStatus.FAILED:
                failed.add(job_id)

        pending -= completed
        if pending:
            time.sleep(_POLL_INTERVAL_SECONDS)

    return failed
