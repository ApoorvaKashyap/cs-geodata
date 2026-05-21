import asyncio
import contextlib
import json
import re
from pathlib import Path

import polars as pl
import polars_st as st
from loguru import logger

from src.app.models import LayerConversionRequest
from src.conversion.helpers.api import convert_base
from src.conversion.helpers.cleaners import (
    classify_columns,
    prefix_cols,
    split_cols,
    unnest_json_cols,
)
from src.conversion.helpers.duckdb_funcs import init_duckdb
from src.conversion.helpers.geojoin import fill_missing_admin_boundaries
from src.conversion.helpers.merge import (
    _get_missing_mws_ids,
    merge_all_layers,
)
from src.conversion.helpers.scheduler import get_all_geojsons, poll_completion
from src.utils.configs import settings

# Columns that are always written to the static parquet (never classified as temporal).
COMMON_COLS = [
    "mws_id",
    "geometry",
    "tehsil",
    "district",
    "state",
    "area_in_ha",
]

# Regex used to extract the leading four-digit year from a year suffix
_FIRST_YEAR_RE = re.compile(r"\d{4}")
# Regex used to extract the trailing ISO date from a fortnightly column name
_DATE_SUFFIX_RE = re.compile(r"(\d{4}-\d{2}-\d{2})$")
# Regex used to extract the trailing year / year-range from an annual column name
_YEAR_SUFFIX_RE = re.compile(r"(\d{4}[_-]\d{4}|\d{4})$")

# Set to an integer to limit the number of tehsils for testing, or None for production.
TEST_LIMIT_TEHSILS: int | None = None


async def run_mws_pipeline(request: LayerConversionRequest) -> None:
    """Run the main MWS data pipeline to merge layers onto a base dataset.

    This function fetches tehsils filtered by the requested version range,
    downloads the base layer, processes requested additional layers, merges
    them, fills missing admin boundaries, and then splits the final dataset
    into three flat Parquet files:

    * ``static.parquet``   — geometry + identity + non-temporal attributes
    * ``fortnightly/``     — melted long, one row per (mws_id, date), partitioned by year
    * ``annual/``          — melted long, one row per (mws_id, year), partitioned by year

    Args:
        request: Configuration for the pipeline run, including layers, paths,
            version bounds, and column mappings.
    """
    import tempfile

    tmpdir = tempfile.mkdtemp()
    logger.info(f"Using temp directory: {tmpdir}")

    logger.info(f"Fetching layer version CSV from {request.layer_version}")
    tehsils = await _fetch_version(request.layer_version)

    # Filter tehsils to the requested version range
    logger.info(
        f"Filtering tehsils to version range [{request.min_version}, {request.max_version}]"
    )
    tehsils = tehsils.filter(
        (pl.col("version") >= request.min_version)
        & (pl.col("version") <= request.max_version)
    )

    if TEST_LIMIT_TEHSILS is not None:
        logger.warning(f"TESTING MODE: Limiting to {TEST_LIMIT_TEHSILS} tehsils.")
        tehsils = tehsils.head(TEST_LIMIT_TEHSILS)

    logger.info("Fetching base layer")
    base_descriptor = request.base_layer_descriptor
    if base_descriptor.source is None:
        raise ValueError(
            f"Base layer '{base_descriptor.name}' has no 'source' path in the descriptor."
        )
    base = await _fetch_base(
        base_descriptor.source,
        super_layer_source=request.super_layer_source,
        super_field=request.super_field,
    )

    # Build rename map from the TOML descriptor.
    # geom→geometry is always enforced as a standardisation step so that the
    # rest of the pipeline can assume a consistent geometry column name,
    # regardless of what the source file calls it.
    base_rename = {**base_descriptor.rename}
    base_rename.setdefault("geom", "geometry")

    base = (
        base.drop(base_descriptor.drop, strict=False)
        .rename(base_rename, strict=False)
        .with_columns(
            st.geom("geometry").st.set_srid(4326).st.to_wkb().alias("geometry")  # type: ignore[attr-defined]
        )
        .collect(engine="streaming")
        .lazy()
    )

    logger.info("Processing layers")
    layer_results = await _process_layer(request, tehsils)

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
    if request.partition_by:
        null_count = (
            base.filter(pl.col(request.partition_by).is_null())
            .collect(engine="streaming")
            .height
        )
        if null_count > 0:
            logger.warning(
                f"Dropping {null_count} rows with null {request.partition_by} "
                f"(out of coverage area)"
            )
            base = base.filter(pl.col(request.partition_by).is_not_null())

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

    logger.info(f"Writing split Parquet outputs to {request.output_path}")
    all_cols = pl.scan_parquet(merged_path).collect_schema().names()
    await _write_split_parquets(
        merged_path, request.output_path, all_cols, request.partition_by
    )

    # Cleanup temp files
    logger.info(f"Cleaning up temp directory: {tmpdir}")
    import shutil

    shutil.rmtree(tmpdir, ignore_errors=True)
    for path in Path(settings.temp_path).glob("*.parquet"):
        path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------


async def _write_split_parquets(
    merged_path: str,
    output_path: str,
    all_cols: list[str],
    partition_by: str | None = None,
) -> None:
    """Classify columns and write static, fortnightly, and annual Parquet files.

    Reads from a local merged Parquet file and writes output via DuckDB's
    native partitioned COPY, which handles Hive-style directory trees and
    S3 writes natively without loading the full dataset into memory.

    When *partition_by* is set (e.g. ``'sub_basin'``) every output is written
    as a Hive-partitioned directory tree with that column as the outer level:

    * ``static/{partition_by}={val}/part-0.parquet``
    * ``fortnightly/{partition_by}={val}/year=YYYY/``
    * ``annual/{partition_by}={val}/year=YYYY/``

    Args:
        merged_path: Path to the local materialized merged Parquet file.
        output_path: Target S3 or local directory for all output files.
        all_cols: List of all column names in the merged frame.
        partition_by: Optional outer Hive partition column name.
    """
    keep_always = [c for c in COMMON_COLS if c in all_cols]
    if partition_by and partition_by not in keep_always:
        keep_always = keep_always + [partition_by]
    static_cols, fortnightly_cols, annual_cols = classify_columns(all_cols, keep_always)

    logger.info(
        f"Column classification — static: {len(static_cols)}, "
        f"fortnightly: {len(fortnightly_cols)}, annual: {len(annual_cols)}"
    )

    conn = init_duckdb()
    try:
        conn.execute(
            f"CREATE TABLE merged AS SELECT * FROM read_parquet('{merged_path}')"
        )

        # ---- static GeoParquet ------------------------------------------------
        logger.info(f"Writing static GeoParquet → {output_path}/static/")
        await _write_static_geoparquet_duckdb(
            conn, static_cols, f"{output_path}/static", partition_by
        )

        # ---- fortnightly -------------------------------------------------------
        if fortnightly_cols:
            non_geo_keep = [c for c in keep_always if c != "geometry"]
            logger.info(f"Writing fortnightly parquet → {output_path}/fortnightly/")
            _write_temporal_parquet_duckdb(
                conn,
                "fortnightly",
                fortnightly_cols,
                non_geo_keep,
                f"{output_path}/fortnightly",
                partition_by,
            )
        else:
            logger.info("No fortnightly columns detected — skipping fortnightly output")

        # ---- annual ------------------------------------------------------------
        if annual_cols:
            non_geo_keep = [c for c in keep_always if c != "geometry"]
            logger.info(f"Writing annual parquet → {output_path}/annual/")
            _write_temporal_parquet_duckdb(
                conn,
                "annual",
                annual_cols,
                non_geo_keep,
                f"{output_path}/annual",
                partition_by,
            )
        else:
            logger.info("No annual columns detected — skipping annual output")

    finally:
        conn.close()


async def _write_static_geoparquet_duckdb(
    conn,
    static_cols: list[str],
    dir_path: str,
    partition_by: str | None = None,
) -> None:
    """Write the static columns as GeoParquet file(s) via DuckDB.

    Adds a bbox struct column alongside the native geometry column so that
    readers can use it for spatial filtering. Writes directly to S3 or local
    filesystem using DuckDB's COPY statement with optional PARTITION BY.

    Args:
        conn: An open DuckDB connection with a 'merged' table registered.
        static_cols: List of column names to include in the static output.
        dir_path: Destination directory path (S3 or local).
        partition_by: Optional Hive partition column.
    """
    # Build the SELECT — geometry needs special treatment to add bbox
    geo_exprs = [
        "* EXCLUDE (geometry)",
        "ST_SetCRS(ST_GeomFromWKB(geometry), 'EPSG:4326') AS geometry",
        "struct_pack("
        "    xmin := ST_XMin(ST_GeomFromWKB(geometry)),"
        "    ymin := ST_YMin(ST_GeomFromWKB(geometry)),"
        "    xmax := ST_XMax(ST_GeomFromWKB(geometry)),"
        "    ymax := ST_YMax(ST_GeomFromWKB(geometry))"
        ") AS bbox",
    ]
    col_select = ", ".join(f'"{c}"' for c in static_cols if c != "geometry")
    if not col_select:
        col_select = "*"

    partition_clause = f"PARTITION_BY ({partition_by})" if partition_by else ""

    sql = f"""
        COPY (
            SELECT
                {col_select.rstrip(", ")},
                ST_SetCRS(ST_GeomFromWKB(geometry), 'EPSG:4326') AS geometry,
                struct_pack(
                    xmin := ST_XMin(ST_GeomFromWKB(geometry)),
                    ymin := ST_YMin(ST_GeomFromWKB(geometry)),
                    xmax := ST_XMax(ST_GeomFromWKB(geometry)),
                    ymax := ST_YMax(ST_GeomFromWKB(geometry))
                ) AS bbox
            FROM merged
        )
        TO '{dir_path}'
        WITH (
            FORMAT 'PARQUET',
            ROW_GROUP_SIZE 100000,
            COMPRESSION 'ZSTD',
            OVERWRITE_OR_IGNORE true
            {(", PARTITION_BY (" + partition_by + ")") if partition_by else ""}
        );
    """
    logger.debug(f"Static COPY SQL:\n{sql}")
    conn.execute(sql)
    logger.info(f"Static GeoParquet written to {dir_path}")


def _write_temporal_parquet_duckdb(
    conn,
    kind: str,
    temporal_cols: list[str],
    keep_cols: list[str],
    base_path: str,
    partition_by: str | None = None,
) -> None:
    """Write melted temporal (fortnightly or annual) output via DuckDB COPY.

    Groups columns by their date/year suffix, constructs a UNION ALL query that
    emits one row per (mws_id, date/year) and writes directly to partitioned
    Parquet files using DuckDB's native COPY statement.

    Args:
        conn: An open DuckDB connection with a 'merged' table registered.
        kind: Either ``'fortnightly'`` or ``'annual'``.
        temporal_cols: The list of wide temporal column names to melt.
        keep_cols: Identity columns to carry forward in each output row.
        base_path: Root output directory (S3 or local).
        partition_by: Optional outer Hive partition column.
    """
    if kind == "fortnightly":
        groups = _group_fortnightly_cols(temporal_cols)
    else:
        groups = _group_annual_cols(temporal_cols)

    if not groups:
        logger.warning(f"No {kind} groups found — skipping")
        return

    keep_select = ", ".join(f'"{c}"' for c in keep_cols)

    # Collect the complete set of variable names across all time groups so that
    # every UNION ALL branch has the same column count (missing vars → NULL).
    all_vars: list[str] = []
    for var_map in groups.values():
        for var in var_map:
            if var not in all_vars:
                all_vars.append(var)

    union_parts: list[str] = []

    for time_val, var_map in groups.items():
        var_select = ", ".join(
            f'"{var_map[var]}" AS "{var}"' if var in var_map else f'NULL AS "{var}"'
            for var in all_vars
        )
        if kind == "fortnightly":
            time_expr = f"DATE '{time_val}' AS date, {int(time_val[:4])} AS year"
        else:
            first_year = _FIRST_YEAR_RE.search(time_val)
            year_val = int(first_year.group()) if first_year else 0
            time_expr = f"{year_val} AS year"

        union_parts.append(
            f"SELECT {keep_select}, {time_expr}, {var_select} FROM merged"
        )

    full_query = " UNION ALL ".join(union_parts)

    partition_cols = []
    if partition_by:
        partition_cols.append(partition_by)
    partition_cols.append("year")
    partition_clause = f"PARTITION_BY ({', '.join(partition_cols)})"

    sql = f"""
        COPY (
            {full_query}
        )
        TO '{base_path}'
        WITH (
            FORMAT 'PARQUET',
            ROW_GROUP_SIZE 100000,
            COMPRESSION 'ZSTD',
            OVERWRITE_OR_IGNORE true,
            {partition_clause}
        );
    """
    logger.debug(f"{kind} COPY SQL (first 500 chars): {sql[:500]}")
    conn.execute(sql)
    logger.info(f"{kind.capitalize()} output written to {base_path}")


def _group_fortnightly_cols(cols: list[str]) -> dict[str, dict[str, str]]:
    """Group fortnightly column names by their ISO date suffix.

    Returns:
        ``{date_str -> {var_name -> orig_col_name}}``
    """
    groups: dict[str, dict[str, str]] = {}
    for col in cols:
        m = _DATE_SUFFIX_RE.search(col)
        if not m:
            continue
        date_str = m.group(1)
        prefix = col[: -(len(date_str) + 1)] if col.endswith("_" + date_str) else col
        groups.setdefault(date_str, {})[prefix] = col
    return groups


def _group_annual_cols(cols: list[str]) -> dict[str, dict[str, str]]:
    """Group annual column names by their year / year-range suffix.

    Returns:
        ``{year_suffix -> {var_name -> orig_col_name}}``
    """
    groups: dict[str, dict[str, str]] = {}
    for col in cols:
        m = _YEAR_SUFFIX_RE.search(col)
        if not m:
            continue
        year_suffix = m.group(1)
        prefix = (
            col[: -(len(year_suffix) + 1)] if col.endswith("_" + year_suffix) else col
        )
        groups.setdefault(year_suffix, {})[prefix] = col
    return groups


# ---------------------------------------------------------------------------
# Layer processing helpers
# ---------------------------------------------------------------------------


async def _process_layer(
    request: LayerConversionRequest,
    tehsils: pl.LazyFrame,
) -> dict[str, pl.LazyFrame]:
    """Dispatch per-tehsil download-and-convert worker tasks, then lazily scan results.

    Each RQ worker downloads the GeoJSON for one tehsil, applies column
    cleaning and admin-boundary tagging, and writes a Parquet file to
    ``settings.temp_path``.  Once all workers have finished the main process
    reads those files back as a single lazy glob scan per layer — no
    in-process GDAL parsing, no large in-memory concat.

    Args:
        request: The pipeline request containing layer configurations and
            per-layer column mappings.

    Returns:
        A dictionary mapping layer names to lazy Parquet glob scans.

    Raises:
        ValueError: If no Parquet files were produced for a layer (e.g. all
            worker tasks failed).
    """
    results: dict[str, pl.LazyFrame] = {}

    work = await get_all_geojsons(request.attribute_layers, tehsils)

    while True:
        completed = await poll_completion(work)
        if completed:
            break
        await asyncio.sleep(5)

    for descriptor in request.attribute_layers:
        layer = descriptor.name
        parquet_glob = f"{settings.temp_path}/{layer}_*.parquet"
        matching = list(Path(settings.temp_path).glob(f"{layer}_*.parquet"))
        if not matching:
            raise ValueError(
                f"No Parquet files found for layer '{layer}' — "
                "all worker tasks may have failed."
            )
        logger.info(f"Scanning {len(matching)} Parquet file(s) for layer '{layer}'")
        lazy_frames = [pl.scan_parquet(p) for p in matching]
        results[layer] = pl.concat(lazy_frames, how="diagonal_relaxed")

    return results


async def _fetch_version(s3_path: str) -> pl.LazyFrame:
    """Fetch the layer version metadata CSV from S3.

    Args:
        s3_path: The S3 path to the layer version CSV.

    Returns:
        A lazy dataframe containing the version metadata sorted by state.
    """
    df = pl.read_csv(s3_path)
    return (
        df.rename(
            {
                "State": "state_name",
                "District": "district_name",
                "Tehsil": "tehsil_name",
                "Layer Version": "version",
            }
        )
        .sort("state_name")
        .lazy()
    )


async def _fetch_base(
    base_layer: str,
    super_layer_source: str | None = None,
    super_field: str | None = None,
) -> pl.LazyFrame:
    """Fetch the base MWS layer, converting it to Parquet if needed.

    When *super_layer_source* and *super_field* are provided, a
    centroid-in-polygon spatial join is also performed during conversion
    to assign the super-layer field to every base entity row.

    Args:
        base_layer: Path or URI to the base layer.
        super_layer_source: Optional super-layer boundary file path/URI.
        super_field: Column name in the super-layer to copy onto each row.

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
    success = await convert_base(
        base_layer,
        converted_path,
        super_layer_source=super_layer_source,
        super_field=super_field,
    )

    if not success:
        raise ValueError(f"Failed to convert base layer: {base_layer}")

    return pl.scan_parquet(converted_path)


if __name__ == "__main__":
    logger.add("logs/mws.log")
    with open("examples/mws.json") as f:
        request = json.load(f)
    request = LayerConversionRequest(**request)
    asyncio.run(run_mws_pipeline(request))
