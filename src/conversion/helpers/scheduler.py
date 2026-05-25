import polars as pl
from loguru import logger
from rq.job import Job

from src.app.models import LayerDescriptor
from src.conversion.helpers.api import download_and_convert_geojson
from src.conversion.helpers.cleaners import clean_label
from src.work.work_queue import get_status, mq


async def _create_tehsil_map(
    descriptor: LayerDescriptor,
    tehsils_t: pl.DataFrame,
) -> dict[str, Job]:
    """Enqueue GeoJSON download-and-convert tasks for one layer across all active tehsils.

    Each enqueued task downloads the GeoJSON, cleans the columns, tags admin
    boundaries, and writes a Parquet file — all inside the RQ worker process.

    Args:
        descriptor: The layer descriptor containing the URL template and column mapping.
        tehsils_t: DataFrame containing active tehsil information.

    Returns:
        A dictionary mapping task keys to rq Job objects.
    """
    tmap: dict[str, Job] = {}

    for row in tehsils_t.to_dicts():
        state_slug = clean_label(row["state_name"])
        district_slug = clean_label(row["district_name"])
        tehsil_slug = clean_label(row["tehsil_name"])
        task = mq.enqueue(
            download_and_convert_geojson,
            descriptor.name,
            district_slug,
            tehsil_slug,
            row["state_name"],
            row["district_name"],
            row["tehsil_name"],
            descriptor.url_template,
            descriptor.rename,
            descriptor.drop,
            job_timeout=3600,
        )
        tmap[f"{descriptor.name}_{state_slug}_{district_slug}_{tehsil_slug}"] = task

    logger.info(
        f"Enqueued {len(tmap)} jobs for layer '{descriptor.name}' "
        f"({tehsils_t.height} tehsils in input)"
    )
    return tmap


async def get_all_geojsons(
    attribute_layers: list[LayerDescriptor],
    tehsils: pl.LazyFrame,
) -> dict:
    """Queue download-and-convert tasks for all WFS collection layers.

    Args:
        attribute_layers: List of non-base LayerDescriptors (type='collection').
        tehsils: The filtered active tehsils to process.

    Returns:
        A dictionary mapping layer names to their corresponding tehsil task maps.
    """
    tehsils_t = tehsils.collect(engine="streaming")
    all_geojsons: dict = {}

    for descriptor in attribute_layers:
        tmap = await _create_tehsil_map(descriptor, tehsils_t)
        logger.info(f"Created tehsil map for layer {descriptor.name}")
        all_geojsons[descriptor.name] = tmap

    return all_geojsons


async def _get_task_completion(layer: dict[str, Job]) -> tuple[int, int, int, int]:
    """Get the completion status counts for a layer's download tasks.

    Args:
        layer: Dictionary of task IDs to Job objects.

    Returns:
        A tuple of (completed, failed, in_progress, pending) counts.
    """
    completed = 0
    failed = 0
    in_progress = 0
    pending = 0

    for i in layer.values():
        status = await get_status(i.id)
        if status["status"] == "finished":
            completed += 1
        elif status["status"] == "failed":
            failed += 1
        elif status["status"] == "started":
            in_progress += 1
        elif status["status"] == "queued":
            pending += 1

    return (completed, failed, in_progress, pending)


async def poll_completion(layers: dict[str, dict[str, Job]]) -> bool:
    """Check if all queued tasks for the given layers have completed.

    Args:
        layers: Dictionary mapping layer names to their task maps.

    Returns:
        True if all tasks have finished or failed, False if any are still
        pending or in progress.
    """
    for layer in layers:
        c, f, i, p = await _get_task_completion(layers[layer])
        if p > 0 or i > 0:
            return False
    return True
