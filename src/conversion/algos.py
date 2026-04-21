import asyncio
import contextlib
import json
from pathlib import Path

import polars as pl
import polars_st as st
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
from src.conversion.helpers.geojoin import fill_missing_admin_boundaries
from src.conversion.helpers.merge import (
    _get_missing_mws_ids,
    merge_all_layers,
    merge_tehsils_on_layer,
)
from src.conversion.helpers.scheduler import get_all_geojsons, poll_completion
from src.utils.configs import settings

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
    """Run the main MWS data pipeline to merge layers onto a base dataset.

    This function fetches active tehsils, downloads the base layer, processes
    requested additional layers, merges them, fills missing admin boundaries,
    and sinks the final dataset to partitioned GeoParquet.

    Args:
        request: Configuration for the pipeline run, including layers, paths,
            and column mappings.
    """
    import tempfile

    tmpdir = tempfile.mkdtemp()
    logger.info(f"Using temp directory: {tmpdir}")

    logger.info(f"Fetching layer version {request.layer_version}")
    version = await _fetch_version(request.layer_version)

    logger.info("Fetching active tehsils")
    tehsils = clean_tehsils(await get_active())
    tehsils = merge_col_metadata(version=version, tehsils=tehsils)

    logger.info("Fetching base layer")
    base = await _fetch_base(next(iter(request.base_layer.values())))
    base = (
        base.with_columns(pl.lit(1.2).alias("version"))
        .rename({"uid": "mws_id", "geom": "geometry"})
        .with_columns(
            st.geom("geometry").st.set_srid(4326).st.to_wkb().alias("geometry")  # type: ignore[attr-defined]
        )
        .collect(engine="streaming")
        .lazy()
    )

    logger.info("Processing layers")
    layer_results = await _process_layer(tehsils, request)

    logger.info("Post-processing and materializing layers")
    for layer in layer_results:
        layer_results[layer] = split_cols(layer_results[layer])
        layer_results[layer] = unnest_json_cols(layer_results[layer])
        layer_results[layer] = prefix_cols(layer_results[layer], layer, COMMON_COLS)

        layer_path = f"{tmpdir}/{layer}.parquet"
        logger.info(f"Sinking layer '{layer}' to {layer_path}")
        layer_results[layer].sink_parquet(layer_path, compression="zstd")
        layer_results[layer] = pl.scan_parquet(layer_path)
        logger.info(f"Layer '{layer}' materialized")

    # Log coverage
    missing = _get_missing_mws_ids(base, layer_results)
    missing_df = missing.collect(engine="streaming")
    if missing_df.height > 0:
        logger.warning(
            f"{missing_df.height} MWSv2 polygons are outside active tehsils — "
            f"they will appear with null layer values. "
            f"Sample IDs: {missing_df['mws_id'].head(5).to_list()}"
        )

    logger.info("Merging all layers onto base")
    merged = merge_all_layers(layer_results, base)

    # Sink merged frame to temp parquet to break the join plan
    merged_path = f"{tmpdir}/merged.parquet"
    logger.info(f"Sinking merged frame to {merged_path}")
    merged.with_columns(
        st.geom("geometry").st.to_wkb().alias("geometry")  # type: ignore[attr-defined]
    ).sink_parquet(merged_path, compression="zstd", row_group_size=100_000)
    logger.info("Merged frame materialized")

    # Reload as lazy for admin boundary fill
    merged = pl.scan_parquet(merged_path)

    # Fill admin boundaries for polygons outside active tehsils
    if missing_df.height > 0:
        logger.info("Adding State, District and Tehsil Data")
        merged = fill_missing_admin_boundaries(
            merged,
            tehsils_path=settings.tehsil_bounds,
        )

    logger.info(f"Writing GeoParquet to {request.output_path}")
    await _write_geoparquet(merged, request.output_path)

    # Cleanup temp files
    logger.info(f"Cleaning up temp directory: {tmpdir}")
    import shutil

    shutil.rmtree(tmpdir, ignore_errors=True)
    for path in Path(settings.temp_path).glob("**/*.geojson"):
        path.unlink()


async def _write_geoparquet(merged: pl.LazyFrame, output_path: str) -> None:
    """Write the merged dataframe to partitioned GeoParquet using DuckDB.

    Computes global bounding box, partitions data by state, and writes
    GeoParquet files compatible with spec 1.1.0.

    Args:
        merged: The final merged lazy dataframe to write.
        output_path: Target directory path for the partitioned output.
    """

    # Compute global bounds across all partitions first
    logger.info("Computing global geometry bounds")
    bounds_df = merged.select(
        st.geom("geometry").st.bounds().alias("bounds")  # type: ignore[attr-defined]
    ).collect(engine="streaming")

    global_bbox: list[float] = [
        float(bounds_df["bounds"].arr.get(0).min()),  # xmin
        float(bounds_df["bounds"].arr.get(1).min()),  # ymin
        float(bounds_df["bounds"].arr.get(2).max()),  # xmax
        float(bounds_df["bounds"].arr.get(3).max()),  # ymax
    ]
    logger.info(f"Global bounds: {global_bbox}")

    # Get distinct states
    states = merged.select("state").unique().collect()["state"].to_list()

    logger.info(f"Writing {len(states)} state partitions to {output_path}")

    conn = init_duckdb()
    try:
        for state in states:
            if state is None:
                partition_key = "unknown"
                state_filter = pl.col("state").is_null()
            else:
                partition_key = state.replace(" ", "_").replace("/", "_")
                state_filter = pl.col("state") == state

            partition_df = merged.filter(state_filter).collect(engine="streaming")
            logger.info(
                f"Partition {partition_key}: {partition_df.height} rows, schema sample:"
            )
            logger.info(f"  geometry dtype: {partition_df['geometry'].dtype}")
            logger.info(
                f"  geometry null count: {partition_df['geometry'].null_count()}"
            )
            logger.info(f"  geometry sample: {partition_df['geometry'].head(2)}")

            if partition_df.is_empty():
                logger.warning(f"Empty partition for state={partition_key}, skipping")
                continue  # don't write empty files

            partition_path = f"{output_path}/state={partition_key}"
            file_path = f"{partition_path}/part-0.parquet"

            logger.info(f"Writing partition: state={partition_key}")

            partition_df = merged.filter(state_filter).collect(engine="streaming")

            if partition_df.is_empty():
                logger.warning(f"Empty partition for state={partition_key}, skipping")
                continue

            Path(partition_path).mkdir(parents=True, exist_ok=True)

            arrow_table = partition_df.to_arrow()
            conn.register("partition_table", arrow_table)

            row = conn.execute("SELECT COUNT(*) FROM partition_table").fetchone()
            count = row[0] if row else 0
            logger.info(f"DuckDB sees {count} rows in partition_table")

            # Write partition — bbox as minx/miny/maxx/maxy struct columns
            # per GeoParquet spec recommendation
            sql = f"""
                COPY (
                    SELECT
                        * EXCLUDE (geometry, state),
                        ST_SetCRS(ST_GeomFromWKB(geometry), 'EPSG:4326') AS geometry,
                        struct_pack(
                            xmin := ST_XMin(ST_GeomFromWKB(geometry)),
                            ymin := ST_YMin(ST_GeomFromWKB(geometry)),
                            xmax := ST_XMax(ST_GeomFromWKB(geometry)),
                            ymax := ST_YMax(ST_GeomFromWKB(geometry))
                        ) AS bbox
                    FROM partition_table
                )
                TO '{file_path}'
                WITH (
                    FORMAT 'PARQUET',
                    ROW_GROUP_SIZE 100000,
                    COMPRESSION 'ZSTD'
                );
            """
            logger.debug(f"Executing SQL:\n{sql}")
            conn.execute(sql)

            conn.unregister("partition_table")

            # Inject correct global bbox into GeoParquet metadata
            # DuckDB writes per-file bbox — we override with global bounds
            _fix_geoparquet_metadata(file_path, global_bbox)

            del arrow_table, partition_df
            logger.info(f"Written partition state={partition_key}")

        logger.info(f"GeoParquet dataset written successfully to {output_path}")

    finally:
        conn.close()
        import glob

        for f in glob.glob("/tmp/duckdb_*.db"):
            with contextlib.suppress(Exception):
                Path(f).unlink()


def _fix_geoparquet_metadata(file_path: str, global_bbox: list[float]) -> None:
    """Override the per-file bbox in GeoParquet metadata with the global bbox.

    This spans all partitions and upgrades to spec version 1.1.0, ensuring
    correct metadata for the `bbox` struct column.

    Args:
        file_path: Path to the written Parquet file.
        global_bbox: Bounding box [xmin, ymin, xmax, ymax] covering all data.
    """
    import json as _json

    import pyarrow.parquet as pq  # type: ignore[import-untyped]

    # Read the full table (preserves row data)
    table = pq.read_table(file_path)

    existing_meta = table.schema.metadata or {}
    decoded = {k.decode(): v.decode() for k, v in existing_meta.items()}

    # Parse existing geo metadata written by DuckDB
    geo = _json.loads(decoded.get("geo", "{}"))

    # Override bbox with global bounds and upgrade version
    geo["version"] = "1.1.0"
    geo["columns"]["geometry"]["bbox"] = global_bbox
    geo["columns"]["geometry"]["covering"] = {
        "bbox": {
            "xmin": ["bbox", "xmin"],
            "ymin": ["bbox", "ymin"],
            "xmax": ["bbox", "xmax"],
            "ymax": ["bbox", "ymax"],
        }
    }

    decoded["geo"] = _json.dumps(geo)

    # Replace schema metadata and rewrite the full table
    updated_table = table.replace_schema_metadata(decoded)
    pq.write_table(updated_table, file_path, compression="zstd")


async def _process_layer(
    tehsils: pl.LazyFrame,
    request: LayerConversionRequest,
) -> dict[str, pl.LazyFrame]:
    """Process all requested layers by merging tehsil-level GeoJSONs.

    Args:
        tehsils: Dataframe containing active tehsil boundaries/metadata.
        request: The pipeline request containing layer configurations.

    Returns:
        A dictionary mapping layer names to their merged lazy dataframes.
    """
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
    """Fetch the layer version metadata CSV from S3.

    Args:
        s3_path: The S3 path to the layer version CSV.

    Returns:
        A lazy dataframe containing the version metadata sorted by state.
    """
    df = pl.read_csv(s3_path)
    return df.sort("State").lazy()


async def _fetch_base(base_layer: str) -> pl.LazyFrame:
    """Fetch the base MWS layer, converting it to Parquet if needed.

    Args:
        base_layer: Path or URI to the base layer.

    Returns:
        A lazy dataframe of the base layer.

    Raises:
        ValueError: If base layer conversion fails.
    """
    try:
        base = pl.scan_parquet(base_layer)
        _ = base.collect_schema()
        return base
    except Exception:
        logger.warning(f"Failed to scan {base_layer}, attempting conversion...")

    converted_path = str(Path(base_layer).with_suffix(".converted.parquet"))
    success = await convert_base(base_layer, converted_path)

    if not success:
        raise ValueError(f"Failed to convert base layer: {base_layer}")

    return pl.scan_parquet(converted_path)


if __name__ == "__main__":
    logger.add("logs/mws.log")
    with open("examples/mws.json") as f:
        request = json.load(f)
    request = LayerConversionRequest(**request)
    asyncio.run(run_mws_pipeline(request))
