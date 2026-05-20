import asyncio
import contextlib
import json
import re
from pathlib import Path

import polars as pl
import polars_st as st
from loguru import logger

from src.app.models import LayerConversionRequest
from src.conversion.helpers.api import convert_base, get_active
from src.conversion.helpers.cleaners import (
    classify_columns,
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
    version_meta = await _fetch_version(request.layer_version)

    logger.info("Fetching active tehsils")
    tehsils = clean_tehsils(await get_active())
    tehsils = merge_col_metadata(version=version_meta, tehsils=tehsils)

    # Filter tehsils to the requested version range
    logger.info(
        f"Filtering tehsils to version range [{request.min_version}, {request.max_version}]"
    )
    tehsils = tehsils.filter(
        (pl.col("version") >= request.min_version)
        & (pl.col("version") <= request.max_version)
    )

    logger.info("Fetching base layer")
    base = await _fetch_base(next(iter(request.base_layer.values())))
    base = (
        base.rename({"uid": "mws_id", "geom": "geometry"})
        .with_columns(
            st.geom("geometry").st.set_srid(4326).st.to_wkb().alias("geometry")  # type: ignore[attr-defined]
        )
        .collect(engine="streaming")
        .lazy()
    )

    logger.info("Processing layers")
    layer_results = await _process_layer(request)

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

    logger.info(f"Writing split Parquet outputs to {request.output_path}")
    await _write_split_parquets(merged, request.output_path)

    # Cleanup temp files
    logger.info(f"Cleaning up temp directory: {tmpdir}")
    import shutil

    shutil.rmtree(tmpdir, ignore_errors=True)
    for path in Path(settings.temp_path).glob("*.parquet"):
        path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Output writers
# ---------------------------------------------------------------------------


async def _write_split_parquets(merged: pl.LazyFrame, output_path: str) -> None:
    """Classify columns and write static, fortnightly, and annual Parquet files.

    * **static.parquet** — flat GeoParquet (WKB geometry + GeoParquet 1.1.0
      metadata) containing identity columns and non-temporal attributes.
    * **fortnightly/year=YYYY/part-0.parquet** — melted long, one row per
      ``(mws_id, date)``, no geometry column.
    * **annual/year=YYYY/part-0.parquet** — melted long, one row per
      ``(mws_id, year)``, no geometry column.

    Args:
        merged: The final merged lazy dataframe.
        output_path: Target directory for all output files.
    """
    all_cols = merged.collect_schema().names()
    # Only include COMMON_COLS that actually exist in the frame
    keep_always = [c for c in COMMON_COLS if c in all_cols]
    static_cols, fortnightly_cols, annual_cols = classify_columns(all_cols, keep_always)

    logger.info(
        f"Column classification — static: {len(static_cols)}, "
        f"fortnightly: {len(fortnightly_cols)}, annual: {len(annual_cols)}"
    )

    # ---- static GeoParquet ------------------------------------------------
    static_path = f"{output_path}/static.parquet"
    logger.info(f"Writing static GeoParquet → {static_path}")
    await _write_static_geoparquet(merged.select(static_cols), static_path)

    # ---- fortnightly -------------------------------------------------------
    if fortnightly_cols:
        non_geo_keep = [c for c in keep_always if c != "geometry"]
        fortnightly_df = _melt_fortnightly(merged, fortnightly_cols, non_geo_keep)
        fortnightly_base = f"{output_path}/fortnightly"
        logger.info(f"Writing fortnightly parquet → {fortnightly_base}/year=*/part-0.parquet")
        _write_temporal_parquet(fortnightly_df, fortnightly_base)
    else:
        logger.info("No fortnightly columns detected — skipping fortnightly output")

    # ---- annual ------------------------------------------------------------
    if annual_cols:
        non_geo_keep = [c for c in keep_always if c != "geometry"]
        annual_df = _melt_annual(merged, annual_cols, non_geo_keep)
        annual_base = f"{output_path}/annual"
        logger.info(f"Writing annual parquet → {annual_base}/year=*/part-0.parquet")
        _write_temporal_parquet(annual_df, annual_base)
    else:
        logger.info("No annual columns detected — skipping annual output")


async def _write_static_geoparquet(df: pl.LazyFrame, file_path: str) -> None:
    """Write the static columns as a flat GeoParquet file with spec 1.1.0 metadata.

    Computes the global bounding box, writes a single Parquet file via DuckDB
    (which injects native geometry and bbox struct columns), then overrides the
    per-file bbox with the global bounds.

    Args:
        df: LazyFrame containing at minimum a WKB ``geometry`` column.
        file_path: Destination path for the output file.
    """
    # Compute global bounds
    logger.info("Computing global geometry bounds for static parquet")
    bounds_df = df.select(
        st.geom("geometry").st.bounds().alias("bounds")  # type: ignore[attr-defined]
    ).collect(engine="streaming")

    global_bbox: list[float] = [
        float(bounds_df["bounds"].arr.get(0).min()),  # xmin
        float(bounds_df["bounds"].arr.get(1).min()),  # ymin
        float(bounds_df["bounds"].arr.get(2).max()),  # xmax
        float(bounds_df["bounds"].arr.get(3).max()),  # ymax
    ]
    logger.info(f"Global bounds: {global_bbox}")

    Path(file_path).parent.mkdir(parents=True, exist_ok=True)

    conn = init_duckdb()
    try:
        df_collected = df.collect(engine="streaming")
        arrow_table = df_collected.to_arrow()
        conn.register("static_table", arrow_table)

        sql = f"""
            COPY (
                SELECT
                    * EXCLUDE (geometry),
                    ST_SetCRS(ST_GeomFromWKB(geometry), 'EPSG:4326') AS geometry,
                    struct_pack(
                        xmin := ST_XMin(ST_GeomFromWKB(geometry)),
                        ymin := ST_YMin(ST_GeomFromWKB(geometry)),
                        xmax := ST_XMax(ST_GeomFromWKB(geometry)),
                        ymax := ST_YMax(ST_GeomFromWKB(geometry))
                    ) AS bbox
                FROM static_table
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
        conn.unregister("static_table")

        _fix_geoparquet_metadata(file_path, global_bbox)
        logger.info(f"Static GeoParquet written to {file_path}")

    finally:
        conn.close()
        import glob

        for f in glob.glob("/tmp/duckdb_*.db"):
            with contextlib.suppress(Exception):
                Path(f).unlink()


def _write_temporal_parquet(df: pl.LazyFrame, base_path: str) -> None:
    """Write a melted temporal dataframe partitioned by year.

    Each year gets its own subdirectory ``year=YYYY/part-0.parquet``.

    Args:
        df: LazyFrame with a ``year`` column (integer).
        base_path: Root directory for the partitioned output.
    """
    years = df.select("year").unique().collect()["year"].to_list()
    logger.info(f"Writing {len(years)} year partitions to {base_path}")

    for year in sorted(years):
        year_df = df.filter(pl.col("year") == year).drop("year")
        year_path = Path(f"{base_path}/year={year}")
        year_path.mkdir(parents=True, exist_ok=True)
        out_file = str(year_path / "part-0.parquet")
        year_df.sink_parquet(out_file, compression="zstd", compression_level=15)
        logger.info(f"  Written year={year} → {out_file}")


# ---------------------------------------------------------------------------
# Melt helpers
# ---------------------------------------------------------------------------


def _melt_fortnightly(
    df: pl.LazyFrame,
    fortnightly_cols: list[str],
    keep_cols: list[str],
) -> pl.LazyFrame:
    """Melt fortnightly columns into a long frame with a ``date`` column.

    Each unique ISO date found in the column names becomes one row per mws_id.
    Variable names are derived by stripping the trailing ``_YYYY-MM-DD`` suffix.

    Schema: ``[keep_cols..., date (pl.Date), year (pl.Int32), <variables...>]``

    Args:
        df: The merged lazy frame containing fortnightly columns.
        fortnightly_cols: Column names that contain a trailing ISO date.
        keep_cols: Identity columns to carry forward (e.g. mws_id, tehsil …).

    Returns:
        Long LazyFrame with one row per (mws_id, date).
    """
    # Group fortnightly cols by their date suffix
    # date_groups: {date_str -> {var_name -> orig_col_name}}
    date_groups: dict[str, dict[str, str]] = {}
    for col in fortnightly_cols:
        m = _DATE_SUFFIX_RE.search(col)
        if not m:
            continue
        date_str = m.group(1)
        # Strip trailing _YYYY-MM-DD to get variable name
        prefix = col[: -(len(date_str) + 1)] if col.endswith("_" + date_str) else col
        date_groups.setdefault(date_str, {})[prefix] = col

    frames: list[pl.LazyFrame] = []
    for date_str, var_map in date_groups.items():
        year = int(date_str[:4])
        sel = [pl.col(c) for c in keep_cols] + [
            pl.col(orig).alias(var) for var, orig in var_map.items()
        ]
        frames.append(
            df.select(sel)
            .with_columns(
                pl.lit(date_str).str.to_date().alias("date"),
                pl.lit(year).cast(pl.Int32).alias("year"),
            )
        )

    if not frames:
        logger.warning("No fortnightly date groups found — returning empty frame")
        return pl.LazyFrame()

    return pl.concat(frames, how="diagonal_relaxed")


def _melt_annual(
    df: pl.LazyFrame,
    annual_cols: list[str],
    keep_cols: list[str],
) -> pl.LazyFrame:
    """Melt annual columns into a long frame with a ``year`` column.

    Columns are grouped by their trailing year / year-range suffix
    (e.g. ``2019_2020`` or ``2023``). The ``year`` column value is the first
    four-digit year found in that suffix.

    Schema: ``[keep_cols..., year (pl.Int32), <variables...>]``

    Args:
        df: The merged lazy frame containing annual columns.
        annual_cols: Column names that contain a year / year-range.
        keep_cols: Identity columns to carry forward (e.g. mws_id, tehsil …).

    Returns:
        Long LazyFrame with one row per (mws_id, year).
    """
    # Group annual cols by their year/year-range suffix
    # year_groups: {year_suffix -> {var_name -> orig_col_name}}
    year_groups: dict[str, dict[str, str]] = {}
    for col in annual_cols:
        m = _YEAR_SUFFIX_RE.search(col)
        if not m:
            continue
        year_suffix = m.group(1)
        # Strip trailing _<suffix> to get variable name
        prefix = col[: -(len(year_suffix) + 1)] if col.endswith("_" + year_suffix) else col
        year_groups.setdefault(year_suffix, {})[prefix] = col

    frames: list[pl.LazyFrame] = []
    for year_suffix, var_map in year_groups.items():
        first_year_m = _FIRST_YEAR_RE.search(year_suffix)
        first_year = int(first_year_m.group()) if first_year_m else 0
        sel = [pl.col(c) for c in keep_cols] + [
            pl.col(orig).alias(var) for var, orig in var_map.items()
        ]
        frames.append(
            df.select(sel)
            .with_columns(pl.lit(first_year).cast(pl.Int32).alias("year"))
        )

    if not frames:
        logger.warning("No annual year groups found — returning empty frame")
        return pl.LazyFrame()

    return pl.concat(frames, how="diagonal_relaxed")


# ---------------------------------------------------------------------------
# GeoParquet metadata fix (kept for static output)
# ---------------------------------------------------------------------------


def _fix_geoparquet_metadata(file_path: str, global_bbox: list[float]) -> None:
    """Override the per-file bbox in GeoParquet metadata with the global bbox.

    Upgrades spec version to 1.1.0 and sets the ``covering`` field so that
    readers can use the bbox struct column for spatial filtering.

    Args:
        file_path: Path to the written Parquet file.
        global_bbox: Bounding box [xmin, ymin, xmax, ymax] covering all data.
    """
    import json as _json

    import pyarrow.parquet as pq  # type: ignore[import-untyped]

    table = pq.read_table(file_path)

    existing_meta = table.schema.metadata or {}
    decoded = {k.decode(): v.decode() for k, v in existing_meta.items()}

    geo = _json.loads(decoded.get("geo", "{}"))

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

    updated_table = table.replace_schema_metadata(decoded)
    pq.write_table(updated_table, file_path, compression="zstd")


# ---------------------------------------------------------------------------
# Layer processing helpers
# ---------------------------------------------------------------------------


async def _process_layer(
    request: LayerConversionRequest,
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

    work = await get_all_geojsons(request.layers, request.column_map)

    while True:
        completed = await poll_completion(work)
        if completed:
            break
        await asyncio.sleep(5)

    for layer in request.layers:
        parquet_glob = f"{settings.temp_path}/{layer}_*.parquet"
        matching = list(Path(settings.temp_path).glob(f"{layer}_*.parquet"))
        if not matching:
            raise ValueError(
                f"No Parquet files found for layer '{layer}' — "
                "all worker tasks may have failed."
            )
        logger.info(
            f"Scanning {len(matching)} Parquet file(s) for layer '{layer}'"
        )
        results[layer] = pl.scan_parquet(parquet_glob)

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
