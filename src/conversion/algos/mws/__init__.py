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
from src.conversion.helpers.duckdb_funcs import init_duckdb
from src.conversion.helpers.merge import (
    _get_missing_mws_ids,
    merge_all_layers,
    merge_tehsils_on_layer,
)
from src.conversion.helpers.scheduler import get_all_geojsons, poll_completion

COMMON_COLS = [
    "mws_id",
    "version",
    "geometry",
    "tehsil",
    "district",
    "state",
    "area_in_ha",
]


async def run_mws_pipeline(request: LayerConversionRequest) -> None:
    logger.info(f"Fetching layer version {request.layer_version}")
    version = await _fetch_version(request.layer_version)

    logger.info("Fetching active tehsils")
    tehsils = clean_tehsils(await get_active())
    tehsils = merge_col_metadata(version=version, tehsils=tehsils)

    logger.info("Fetching base layer")
    base = await _fetch_base(next(iter(request.base_layer.values())))
    base = base.with_columns(pl.lit(1.2).alias("version")).rename(
        {"uid": "mws_id", "geom": "geometry"}
    )

    logger.info("Processing layers")
    layer_results = await _process_layer(tehsils, request)

    logger.info("Post-processing layers")
    for layer in layer_results:
        layer_results[layer] = split_cols(layer_results[layer])
        layer_results[layer] = unnest_json_cols(layer_results[layer])
        layer_results[layer] = prefix_cols(layer_results[layer], layer, COMMON_COLS)

    # Log coverage — useful to know how many polygons are outside active tehsils
    missing = _get_missing_mws_ids(base, layer_results)
    missing_df = missing.collect(engine="streaming")
    if missing_df.height > 0:
        logger.warning(
            f"{missing_df.height} MWSv2 polygons are outside active tehsils. "
            f"They will appear in output with null layer values. "
            f"Sample IDs: {missing_df['mws_id'].head(5).to_list()}"
        )

    logger.info("Merging all layers onto base")
    merged = merge_all_layers(layer_results, base)

    logger.info(f"Sinking merged parquet to {request.output_path}")
    # final = merged.collect(engine="streaming").to_arrow()
    merged.sink_parquet(request.output_path, compression="zstd", row_group_size=100000)

    # conn = init_duckdb()
    # conn.register("final", final)
    # conn.execute(f"""
    # COPY (
    #     SELECT
    #         * EXCLUDE (geometry),
    #         ST_GeomFromWKB(geometry) AS geometry,
    #         ST_Extent(ST_GeomFromWKB(geometry)) AS bbox
    #     FROM final
    #     ORDER BY ST_Hilbert(ST_GeomFromWKB(geometry))
    # )
    # TO '{request.output_path}'
    # WITH (FORMAT 'PARQUET', ROW_GROUP_SIZE 100000, COMPRESSION 'ZSTD');
    # """)
    # conn.unregister("final")
    # conn.close()


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
