import asyncio
import json

import polars as pl
from loguru import logger

from src.app.models import LayerConversionRequest
from src.conversion.algos.mws.merge import merge_tehsils_on_layer
from src.conversion.helpers.api import get_active
from src.conversion.helpers.cleaners import clean_tehsils
from src.conversion.helpers.scheduler import get_all_geojsons, poll_completion

_mws_layers_version: dict = {}


async def run_mws_pipeline(request: LayerConversionRequest) -> None:
    # Fetch the individual layer versions
    logger.info(f"Fetching layer version {request.layer_version}")
    version = _fetch_version(request.layer_version).lazy()

    # Fetch the list of all active tehsils
    logger.info("Fetching active tehsils")
    tehsils = clean_tehsils(await get_active())

    # Join the tehsils with the layer version
    logger.info("Joining tehsils with layer version")
    tehsils = tehsils.join(version, left_on="tehsil_name", right_on="Tehsil")

    # Process all the layers and return a dataframe per layer which
    # contains all the mws_id of that layer
    logger.info("Processing layers")
    layer_results = await _process_layer(tehsils, request.layers)

    for layer_name, lf in layer_results.items():
        print(f"\n=== {layer_name} ===")
        print(lf.collect())


async def _process_layer(
    tehsils: pl.LazyFrame, layers: list[str]
) -> dict[str, pl.LazyFrame]:
    # Process all the layers and return a dataframe per layer
    results: dict[str, pl.LazyFrame] = {}

    work = await get_all_geojsons(layers)

    while True:
        completed = await poll_completion(work)
        if completed:
            break
        await asyncio.sleep(5)

    for layer in layers:
        logger.info(f"Merging layer {layer}")
        results[layer] = merge_tehsils_on_layer(layer, tehsils)

    return results


def _fetch_version(s3_path: str) -> pl.DataFrame:
    return pl.read_csv(s3_path).sort("State")


if __name__ == "__main__":
    with open("examples/mws.json") as f:
        request = json.load(f)
    request = LayerConversionRequest(**request)
    asyncio.run(run_mws_pipeline(request))
