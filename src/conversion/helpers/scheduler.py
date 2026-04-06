import polars as pl
from loguru import logger

from src.conversion.helpers.api import get_active, get_geojson
from src.conversion.helpers.cleaners import clean_label, clean_tehsils
from src.work.work_queue import get_status, mq


async def _create_tehsil_map(layer: str, tehsils_t: pl.DataFrame) -> dict[str, str]:
    tmap: dict = {}

    for row in tehsils_t.iter_rows():
        task = mq.enqueue(get_geojson, layer, clean_label(row[2]), clean_label(row[4]))
        tmap[f"{layer}_{clean_label(row[2])}_{clean_label(row[4])}"] = task

    return tmap


async def get_all_geojsons(layers: list[str]) -> dict:
    tehsils_t = clean_tehsils(await get_active()).collect()
    all_geojsons: dict = {}

    for layer in layers:
        tmap = await _create_tehsil_map(layer, tehsils_t)
        logger.info(f"Created tehsil map for layer {layer}")
        all_geojsons[layer] = tmap

    return all_geojsons


async def _get_task_completion(layer: dict[str, str]) -> tuple[int, int, int, int]:
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


async def poll_completion(layers: dict[str, dict[str, str]]) -> bool:
    for layer in layers:
        c, f, i, p = await _get_task_completion(layers[layer])
        if p > 0 or i > 0:
            return False
    return True
