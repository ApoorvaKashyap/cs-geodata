import polars as pl
from loguru import logger
from rq.job import Job

from src.app.models import ColumnMapping
from src.conversion.helpers.api import download_and_convert_geojson, get_active
from src.conversion.helpers.cleaners import clean_label, clean_tehsils
from src.work.work_queue import get_status, mq


async def _create_tehsil_map(
    layer: str,
    tehsils_t: pl.DataFrame,
    column_mapping: ColumnMapping,
) -> dict[str, Job]:
    """Enqueue GeoJSON download-and-convert tasks for a layer across all active tehsils.

    Each enqueued task downloads the GeoJSON, cleans the columns, tags admin
    boundaries, and writes a Parquet file — all inside the RQ worker process.

    Args:
        layer: The name of the layer.
        tehsils_t: DataFrame containing tehsil information.
        column_mapping: Per-layer column rename/drop configuration.

    Returns:
        A dictionary mapping task IDs to rq Job objects.
    """
    tmap: dict[str, Job] = {}

    for row in tehsils_t.to_dicts():
        district_slug = clean_label(row["district_name"])
        tehsil_slug = clean_label(row["tehsil_name"])
        task = mq.enqueue(
            download_and_convert_geojson,
            layer,
            district_slug,
            tehsil_slug,
            row["state_name"],
            row["district_name"],
            row["tehsil_name"],
            column_mapping.rename_columns,
            column_mapping.drop_columns,
        )
        tmap[f"{layer}_{district_slug}_{tehsil_slug}"] = task

    return tmap


async def get_all_geojsons(
    layers: list[str],
    column_map: dict[str, ColumnMapping],
) -> dict:
    """Queue GeoJSON download-and-convert tasks for multiple layers.

    Args:
        layers: List of layer names to download.
        column_map: Mapping of layer name to its column rename/drop configuration.

    Returns:
        A dictionary mapping layer names to their corresponding tehsil task maps.
    """
    tehsils_t = clean_tehsils(await get_active()).collect(engine="streaming")
    all_geojsons: dict = {}

    for layer in layers:
        mapping = column_map.get(layer, ColumnMapping())
        tmap = await _create_tehsil_map(layer, tehsils_t, mapping)
        logger.info(f"Created tehsil map for layer {layer}")
        all_geojsons[layer] = tmap

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
