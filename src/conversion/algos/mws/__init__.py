import asyncio
import json
from pathlib import Path

import polars as pl
from loguru import logger

from src.app.models import LayerConversionRequest
from src.conversion.helpers.api import convert_base, get_active
from src.conversion.helpers.cleaners import (
    clean_tehsils,
    merge_col_metadata,
    prefix_cols,
    split_cols,
    unnest_json_cols,
)
from src.conversion.helpers.merge import merge_tehsils_on_layer
from src.conversion.helpers.scheduler import get_all_geojsons, poll_completion


async def run_mws_pipeline(request: LayerConversionRequest) -> None:
    # Fetch the individual layer versions
    logger.info(f"Fetching layer version {request.layer_version}")
    version = await _fetch_version(request.layer_version)

    # Fetch the list of all active tehsils
    logger.info("Fetching active tehsils")
    tehsils = clean_tehsils(await get_active())
    tehsils = merge_col_metadata(version=version, tehsils=tehsils)

    # Fetch base layer
    logger.info("Fetching base layer")
    base = await _fetch_base(next(iter(request.base_layer.values())))
    logger.info("Fetched base successfully")
    base_cols = base.collect_schema().names()
    base_cols.extend(["version", "tehsil", "district", "state", "geometry"])

    # Process all the layers and return a dataframe per layer which
    # contains all the mws_id of that layer
    logger.info("Processing layers")
    layer_results = await _process_layer(tehsils, request)

    for layer in layer_results:
        layer_results[layer] = unnest_json_cols(layer_results[layer])
        layer_results[layer] = split_cols(layer_results[layer])
        layer_results[layer] = prefix_cols(layer_results[layer], layer, base_cols)

    for layer in layer_results:
        print(layer, layer_results[layer].collect(engine="streaming").head(5))
        layer_results[layer].sink_parquet(f"{layer}.parquet")


async def _process_layer(
    tehsils: pl.LazyFrame,
    request: LayerConversionRequest,
) -> dict[str, pl.LazyFrame]:
    # Process all the layers and return a dataframe per layer
    results: dict[str, pl.LazyFrame] = {}

    work = await get_all_geojsons(request.layers)

    while True:
        completed = await poll_completion(work)
        if completed:
            break
        await asyncio.sleep(5)

    for layer in request.layers:
        logger.info(f"Merging layer {layer}")
        results[layer] = merge_tehsils_on_layer(
            layer,
            tehsils,
            request.column_map[layer].rename_columns,
            request.column_map[layer].drop_columns,
        )

    return results


async def _fetch_version(s3_path: str) -> pl.LazyFrame:
    df = pl.read_csv(s3_path)
    return df.sort("State").lazy()


async def _fetch_base(base_layer: str) -> pl.LazyFrame:
    try:
        base = pl.scan_parquet(base_layer)
        _ = base.collect_schema()  # force early validation
        return base
    except Exception:
        logger.warning(f"Failed to scan {base_layer}, attempting conversion...")

    converted_path = str(Path(base_layer).with_suffix(".converted.parquet"))
    success = await convert_base(base_layer, converted_path)

    if not success:
        raise ValueError(f"Failed to convert base layer: {base_layer}")

    return pl.scan_parquet(converted_path)


if __name__ == "__main__":
    with open("examples/mws.json") as f:
        request = json.load(f)
    request = LayerConversionRequest(**request)
    asyncio.run(run_mws_pipeline(request))
