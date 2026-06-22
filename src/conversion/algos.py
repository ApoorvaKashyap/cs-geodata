import asyncio
import json
import re
from pathlib import Path

import pyarrow.parquet as pq
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
_DATE_SUFFIX_RE = re.compile(r"(\d{1,4}-\d{1,2}-\d{1,4})$")
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
        request (LayerConversionRequest): Configuration for the pipeline run, including layers, paths,
            version bounds, and column mappings.
    """
    import tempfile

    tmpdir = tempfile.mkdtemp()
    logger.info(f"Using temp directory: {tmpdir}")

    if request.layer_version:
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
        logger.debug(
            f"List of Filtered Tehsils: {tehsils.collect(engine='streaming')['tehsil_name'].to_list()}"
        )
        if settings.test_limit_tehsils is not None:
            logger.warning(
                f"TESTING MODE: Limiting to {settings.test_limit_tehsils} tehsils."
            )
            tehsils = tehsils.head(settings.test_limit_tehsils)
    else:
        logger.info(
            "No layer_version URL provided — skipping tehsil filtering. "
            "Attribute WFS layers will not be fetched."
        )
        tehsils = pl.LazyFrame(
            schema={
                "state_name": pl.Utf8,
                "district_name": pl.Utf8,
                "tehsil_name": pl.Utf8,
                "version": pl.Float64,
                "tehsil": pl.Utf8,
            }
        )

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

    base_cols = base.collect_schema().names()
    logger.info(f"Base initial columns: {base_cols}")
    from src.conversion.helpers.cleaners import expand_rename_globs

    expanded_base_rename = expand_rename_globs(base_cols, base_rename)
    logger.info(f"Base columns to drop: {base_descriptor.drop}")
    logger.info(f"Base rename mapping: {expanded_base_rename}")

    base = (
        base.drop(base_descriptor.drop, strict=False)
        .rename(expanded_base_rename, strict=False)
        .with_columns(
            st.geom("geometry").st.set_srid(4326).st.to_wkb().alias("geometry")  # type: ignore[attr-defined]
        )
    )
    logger.info(f"Base columns after rename/drop: {base.collect_schema().names()}")
    from src.conversion.helpers.cleaners import apply_scaling

    logger.info(f"Applying scale factors: {base_descriptor.scale}")
    base = apply_scaling(base, base_descriptor.scale)

    logger.info("Processing layers")
    layer_results = await _process_layer(request, tehsils)

    logger.info("Post-processing and materializing layers")
    dynamic_common_cols = list(COMMON_COLS)
    if request.key not in dynamic_common_cols:
        dynamic_common_cols.append(request.key)

    for layer in layer_results:
        layer_results[layer] = split_cols(layer_results[layer])
        layer_results[layer] = unnest_json_cols(layer_results[layer])
        layer_results[layer] = prefix_cols(
            layer_results[layer], layer, dynamic_common_cols
        )

        layer_path = f"{tmpdir}/{layer}.parquet"
        logger.info(f"Sinking layer '{layer}' to {layer_path}")
        layer_results[layer].sink_parquet(
            layer_path,
            compression="zstd",
            compression_level=settings.parquet_compression_level,
            row_group_size=settings.parquet_row_group_size,
        )
        layer_results[layer] = pl.scan_parquet(layer_path)
        logger.info(f"Layer '{layer}' materialized")

    # Log coverage
    missing = _get_missing_mws_ids(base, layer_results, entity_key=request.key)
    missing_df = missing.collect(engine="streaming")
    if missing_df.height > 0:
        logger.warning(
            f"{missing_df.height} base polygons are outside active tehsils — "
            f"they will appear with null layer values. "
            f"Sample IDs: {missing_df[request.key].head(5).to_list()}"
        )

    logger.info("Merging all layers onto base")
    if request.partition_by:
        base_schema = base.collect_schema().names()
        if request.partition_by not in base_schema:
            logger.warning(
                f"partition_by column '{request.partition_by}' does not exist on the "
                "base frame — skipping null-row filter. Ensure a super-layer is "
                "configured if you need this column populated."
            )
        else:
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

    merged = merge_all_layers(layer_results, base, entity_key=request.key)

    # Sink merged frame to temp parquet to break the join plan
    merged_path = f"{tmpdir}/merged.parquet"
    logger.info(f"Sinking merged frame to {merged_path}")
    merged.with_columns(
        st.geom("geometry").st.to_wkb().alias("geometry")  # type: ignore[attr-defined]
    ).sink_parquet(
        merged_path,
        compression="zstd",
        compression_level=settings.parquet_compression_level,
        row_group_size=settings.parquet_row_group_size,
    )
    logger.info("Merged frame materialized")

    # Reload as lazy for admin boundary fill
    merged = pl.scan_parquet(merged_path)

    # Fill admin boundaries for polygons outside active tehsils
    if missing_df.height > 0 or request.add_admin:
        logger.info("Adding State, District and Tehsil Data")
        merged = fill_missing_admin_boundaries(
            merged,
            tehsils_path=settings.tehsil_bounds,
            entity_key=request.key,
        )

        # Overwrite the temp merged file so DuckDB output picks up the admin columns
        logger.info(f"Sinking admin-filled merged frame back to {merged_path}")
        tmp_admin_path = f"{merged_path}.admin.tmp"
        merged.sink_parquet(
            tmp_admin_path,
            compression="zstd",
            compression_level=settings.parquet_compression_level,
            row_group_size=settings.parquet_row_group_size,
        )
        import shutil

        shutil.move(tmp_admin_path, merged_path)

    logger.info(f"Writing split Parquet outputs to {request.output_path}")
    all_cols = pl.scan_parquet(merged_path).collect_schema().names()
    await _write_split_parquets(
        merged_path, request.output_path, all_cols, request.key, request.partition_by
    )

    # Cleanup temp files
    logger.info(f"Cleaning up temp directory: {tmpdir}")
    import shutil

    shutil.rmtree(tmpdir, ignore_errors=True)
    for path in Path(settings.temp_path).glob("*.parquet"):
        path.unlink(missing_ok=True)


async def _write_split_parquets(
    merged_path: str,
    output_path: str,
    all_cols: list[str],
    entity_key: str,
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
        merged_path (str): Path to the local materialized merged Parquet file.
        output_path (str): Target S3 or local directory for all output files.
        all_cols (list[str]): List of all column names in the merged frame.
        partition_by (str | None): Optional outer Hive partition column name.
    """
    keep_always = [c for c in COMMON_COLS if c in all_cols]
    if entity_key and entity_key not in keep_always and entity_key in all_cols:
        keep_always.append(entity_key)
    if partition_by and partition_by not in keep_always:
        keep_always.append(partition_by)
    static_cols, fortnightly_cols, annual_cols = classify_columns(all_cols, keep_always)

    logger.info(
        f"Column classification — static: {len(static_cols)}, "
        f"fortnightly: {len(fortnightly_cols)}, annual: {len(annual_cols)}"
    )

    if not output_path.startswith("s3://"):
        import os

        os.makedirs(output_path, exist_ok=True)

    conn = init_duckdb()
    try:
        # VIEW = zero-copy; DuckDB pushes column selection down into the parquet
        # scan so each COPY query only reads the columns it actually needs.
        conn.execute(
            f"CREATE VIEW merged AS SELECT * FROM read_parquet('{merged_path}')"
        )

        logger.info(f"Writing static GeoParquet → {output_path}/static/")
        await _write_static_geoparquet_duckdb(
            conn, static_cols, f"{output_path}/static", partition_by
        )

        if fortnightly_cols:
            non_geo_keep = [c for c in keep_always if c != "geometry"]
            logger.info(f"Writing fortnightly parquet → {output_path}/fortnightly")
            _write_temporal_parquet_polars(
                merged_path,
                "fortnightly",
                fortnightly_cols,
                non_geo_keep,
                f"{output_path}/fortnightly",
                partition_by,
            )
        else:
            logger.info("No fortnightly columns detected — skipping fortnightly output")

        if annual_cols:
            non_geo_keep = [c for c in keep_always if c != "geometry"]
            logger.info(f"Writing annual parquet → {output_path}/annual")
            _write_temporal_parquet_polars(
                merged_path,
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
    readers can use it for spatial filtering.

    For **local** paths DuckDB writes directly to *dir_path*, then every
    output ``.parquet`` file is patched in-place to upgrade the GeoParquet
    ``geo`` metadata from 1.0.0 → 1.1.0 and inject the ``covering.bbox``
    entry.

    For **S3** paths the write-patch-upload workflow is used:

    1. DuckDB writes to a local temporary directory (preserving any Hive
       partition subdirectory structure).
    2. Every file is patched locally with :func:`_patch_geoparquet_metadata`.
    3. The patched files are uploaded to *dir_path* on S3 via ``s3fs``,
       preserving the relative path layout.
    4. The temporary directory is deleted.

    Args:
        conn: An open DuckDB connection with a 'merged' table registered.
        static_cols (list[str]): List of column names to include in the static output.
        dir_path (str): Destination directory path (S3 ``s3://`` or local).
        partition_by (str | None): Optional Hive partition column.
    """
    import shutil
    import tempfile

    is_s3 = dir_path.startswith("s3://")

    # Always write DuckDB output into a local temp directory first.
    # This avoids DuckDB COPY TO path-resolution bugs on some versions when
    # the destination directory was just freshly created by Python.
    # For partitioned writes the whole temp dir is the target; for single-file
    # writes we write part-0.parquet into the temp dir then move it.
    tmp_local = Path(tempfile.mkdtemp(prefix="static_geoparquet_"))
    write_target = str(tmp_local)

    # Ensure the final destination directory exists (local only; S3 is handled by upload).
    if not is_s3:
        Path(dir_path).mkdir(parents=True, exist_ok=True)

    col_select = ", ".join(f'"{c}"' for c in static_cols if c != "geometry")
    if not col_select:
        col_select = "*"

    # Without PARTITION_BY, DuckDB writes a single file — give it an explicit filename.
    copy_target = write_target if partition_by else str(tmp_local / "part-0.parquet")

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
            {(f"ORDER BY {partition_by}") if partition_by else ""}
        )
        TO '{copy_target}'
        WITH (
            FORMAT 'PARQUET',
            ROW_GROUP_SIZE {settings.parquet_row_group_size},
            COMPRESSION 'ZSTD',
            COMPRESSION_LEVEL {settings.parquet_compression_level}
            {(", OVERWRITE_OR_IGNORE true, PARTITION_BY (" + partition_by + ")") if partition_by else ""}
        );
    """
    logger.debug(f"Static COPY SQL:\n{sql}")
    conn.execute(sql)
    logger.info(f"Static GeoParquet written to temp: {write_target}")

    # Patch GeoParquet metadata on every file in the temp dir.
    written = sorted(tmp_local.rglob("*.parquet"))
    if not written:
        logger.warning(f"No .parquet files found under {write_target} to patch.")
    else:
        for parquet_file in written:
            _patch_geoparquet_metadata(parquet_file)
        logger.info(
            f"Patched GeoParquet metadata (v1.1.0 + covering.bbox) "
            f"on {len(written)} file(s)"
        )

    # Move patched files to their final destination.
    try:
        if is_s3:
            n = _upload_dir_to_s3(tmp_local, dir_path)
            logger.info(f"Uploaded {n} patched static file(s) to {dir_path}")
        else:
            # Copy every file preserving the relative Hive subdirectory structure.
            for local_file in written:
                rel = local_file.relative_to(tmp_local)
                dest = Path(dir_path) / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(str(local_file), str(dest))
            logger.info(f"Copied {len(written)} static file(s) to {dir_path}")
    finally:
        shutil.rmtree(tmp_local, ignore_errors=True)


def _patch_geoparquet_metadata(parquet_file: Path) -> None:
    """Upgrade the GeoParquet ``geo`` metadata key inside a Parquet file.

    Performs two upgrades in-place:

    * Bumps ``version`` from ``"1.0.0"`` → ``"1.1.0"``.
    * Injects a ``covering.bbox`` entry into the primary geometry column
      metadata, pointing to the ``bbox`` struct column's four child fields
      (``xmin``, ``ymin``, ``xmax``, ``ymax``).

    Data pages are streamed one row-group at a time so the full 15M-row
    WKB geometry column is never loaded into RAM all at once.

    Args:
        parquet_file (Path): Path to the ``.parquet`` file to patch.
    """
    tmp_path = parquet_file.with_suffix(".patching.parquet")
    try:
        pf = pq.ParquetFile(str(parquet_file), memory_map=True)
        old_schema = pf.schema_arrow
        kv_meta: dict[bytes, bytes] = dict(old_schema.metadata or {})

        geo_key = b"geo"
        if geo_key not in kv_meta:
            logger.warning(
                f"No 'geo' metadata found in {parquet_file.name} — skipping patch."
            )
            return

        geo: dict = json.loads(kv_meta[geo_key].decode())

        # 1. Upgrade version
        geo["version"] = "1.1.0"

        # 2. Add covering.bbox to the primary geometry column entry.
        #    The primary geometry column is identified by geo["primary_column"];
        #    fall back to "geometry" if the key is absent.
        primary_col = geo.get("primary_column", "geometry")
        col_meta: dict = geo.get("columns", {}).get(primary_col, {})
        if "covering" not in col_meta:
            col_meta["covering"] = {
                "bbox": {
                    "xmin": ["bbox", "xmin"],
                    "ymin": ["bbox", "ymin"],
                    "xmax": ["bbox", "xmax"],
                    "ymax": ["bbox", "ymax"],
                }
            }
            geo.setdefault("columns", {})[primary_col] = col_meta

        kv_meta[geo_key] = json.dumps(geo).encode()
        new_schema = old_schema.with_metadata(kv_meta)

        # Stream row-groups one at a time — avoids loading the full WKB geometry
        # column (multi-GB at 15.92M rows) into RAM.
        # The ParquetWriter carries new_schema so the file-level kv_meta
        # (including the updated 'geo' key) is written correctly on close().
        writer = pq.ParquetWriter(
            str(tmp_path),
            new_schema,
            compression="zstd",
            compression_level=settings.parquet_compression_level,
            write_statistics=True,
        )
        try:
            n_groups = pf.metadata.num_row_groups
            for i in range(n_groups):
                writer.write_table(pf.read_row_group(i))
        finally:
            writer.close()

        tmp_path.replace(parquet_file)
        logger.debug(f"Patched GeoParquet metadata on {parquet_file.name}")
    except Exception as exc:
        tmp_path.unlink(missing_ok=True)
        logger.warning(
            f"Failed to patch GeoParquet metadata on {parquet_file.name}: {exc}"
        )


def _upload_dir_to_s3(local_dir: Path, s3_prefix: str) -> int:
    """Upload all ``.parquet`` files under *local_dir* to *s3_prefix* on S3.

    Preserves the relative directory layout so that Hive-partition
    subdirectories (e.g. ``sub_basin=Cauvery/``) survive the upload intact.

    Args:
        local_dir (Path): Root of the local directory tree to upload.
        s3_prefix (str): Target S3 prefix (``s3://bucket/path``).

    Returns:
        int: Number of files uploaded.
    """
    import s3fs

    fs = s3fs.S3FileSystem()
    files = sorted(local_dir.rglob("*.parquet"))
    for local_file in files:
        rel = local_file.relative_to(local_dir)
        target = f"{s3_prefix.rstrip('/')}/{rel.as_posix()}"
        fs.put(str(local_file), target)
        logger.debug(f"Uploaded {rel} → {target}")
    return len(files)


def _write_temporal_parquet_polars(
    merged_path: str,
    kind: str,
    temporal_cols: list[str],
    keep_cols: list[str],
    base_path: str,
    partition_by: str | None = None,
) -> None:
    """Write melted temporal (fortnightly or annual) output using Polars.

    Groups columns by their date/year suffix. Each time-period slice is sunk
    to a temporary Parquet file individually, avoiding an N × 15M-row concat
    plan that would OOM on large datasets. The per-slice files are then scanned
    and re-sunk with Hive partitioning in a single streaming pass.

    Args:
        merged_path (str): Path to the local merged Parquet file.
        kind (str): Either ``'fortnightly'`` or ``'annual'``.
        temporal_cols (list[str]): The list of wide temporal column names to melt.
        keep_cols (list[str]): Identity columns to carry forward in each output row.
        base_path (str): Root output directory (S3 or local).
        partition_by (str | None): Optional outer Hive partition column.
    """
    if kind == "fortnightly":
        groups = _group_fortnightly_cols(temporal_cols)
    else:
        groups = _group_annual_cols(temporal_cols)

    if not groups:
        logger.warning(f"No {kind} groups found — skipping")
        return

    lf = pl.scan_parquet(merged_path)

    # Keep track of variable order for the final projection
    all_vars_ordered: list[str] = []
    for var_map in groups.values():
        for var in var_map.keys():
            if var not in all_vars_ordered:
                all_vars_ordered.append(var)

    # Enforce strict column ordering
    all_final_cols = keep_cols.copy()
    if kind == "fortnightly":
        all_final_cols.extend(["date", "year"])
    else:
        all_final_cols.append("year")
    all_final_cols.extend(all_vars_ordered)

    partition_cols = []
    if partition_by:
        partition_cols.append(partition_by)
    if kind == "fortnightly":
        partition_cols.append("year")

    import shutil
    import tempfile

    is_s3 = base_path.startswith("s3://")
    if is_s3:
        tmp_local = Path(tempfile.mkdtemp(prefix=f"{kind}_geoparquet_"))
        write_target = str(tmp_local)
    else:
        tmp_local = None
        write_target = str(Path(base_path).expanduser())
        Path(write_target).mkdir(parents=True, exist_ok=True)

    # Sink each time-period slice to its own temp parquet individually.
    # This avoids building one giant concat plan (N slices × 15.92M rows)
    # that would materialise everything in RAM before the sink can drain it.
    slices_tmp = Path(tempfile.mkdtemp(prefix=f"{kind}_slices_"))
    slice_paths: list[str] = []
    try:
        for time_val, var_map in groups.items():
            exprs = [pl.col(c) for c in keep_cols]
            if kind == "fortnightly":
                exprs.append(
                    pl.lit(time_val).str.strptime(pl.Date, "%Y-%m-%d").alias("date")
                )
                exprs.append(pl.lit(int(time_val[:4])).cast(pl.Int32).alias("year"))
            else:
                first_year = _FIRST_YEAR_RE.search(time_val)
                year_val = int(first_year.group()) if first_year else 0
                exprs.append(pl.lit(year_val).cast(pl.Int32).alias("year"))

            for var, orig_col in var_map.items():
                exprs.append(pl.col(orig_col).alias(var))

            safe_time = time_val.replace("-", "_").replace("/", "_")
            slice_path = str(slices_tmp / f"slice_{safe_time}.parquet")
            logger.debug(f"Sinking {kind} slice '{time_val}' → {slice_path}")
            lf.select(exprs).sink_parquet(
                slice_path,
                compression="zstd",
                compression_level=settings.parquet_compression_level,
                row_group_size=settings.parquet_row_group_size,
            )
            slice_paths.append(slice_path)

        # Scan all per-slice files (already on disk, narrow columns) and
        # partition-sink in one streaming pass.
        logger.debug(f"Writing {kind} frame to {write_target} via Polars")
        pl.scan_parquet(slice_paths).select(all_final_cols).sink_parquet(
            pl.PartitionBy(write_target, key=partition_cols),
            compression="zstd",
            compression_level=settings.parquet_compression_level,
            row_group_size=settings.parquet_row_group_size,
        )
    finally:
        shutil.rmtree(slices_tmp, ignore_errors=True)

    logger.info(f"{kind.capitalize()} output written to {write_target}")

    if is_s3 and tmp_local is not None:
        try:
            n = _upload_dir_to_s3(tmp_local, base_path)
            logger.info(f"Uploaded {n} {kind} file(s) to {base_path}")
        finally:
            shutil.rmtree(tmp_local, ignore_errors=True)


def _group_fortnightly_cols(cols: list[str]) -> dict[str, dict[str, str]]:
    """Group fortnightly column names by their ISO date suffix.

    Args:
        cols (list[str]): The list of fortnightly column names.

    Returns:
        dict[str, dict[str, str]]: ``{date_str -> {var_name -> orig_col_name}}``
    """
    from dateutil.parser import parse

    groups: dict[str, dict[str, str]] = {}
    for col in cols:
        m = _DATE_SUFFIX_RE.search(col)
        if not m:
            continue

        raw_date_str = m.group(1)
        try:
            date_str = parse(raw_date_str, yearfirst=True).date().isoformat()
        except Exception:
            date_str = raw_date_str

        if col.endswith(raw_date_str):
            prefix = col[: -len(raw_date_str)].rstrip("_")
        else:
            prefix = col

        groups.setdefault(date_str, {})[prefix] = col
    return groups


def _group_annual_cols(cols: list[str]) -> dict[str, dict[str, str]]:
    """Group annual column names by their year / year-range suffix.

    Args:
        cols (list[str]): The list of annual column names.

    Returns:
        dict[str, dict[str, str]]: ``{year_suffix -> {var_name -> orig_col_name}}``
    """
    groups: dict[str, dict[str, str]] = {}
    for col in cols:
        m = _YEAR_SUFFIX_RE.search(col)
        if not m:
            continue
        year_suffix = m.group(1)
        if col.endswith(year_suffix):
            prefix = col[: -len(year_suffix)].rstrip("_")
        else:
            prefix = col
        groups.setdefault(year_suffix, {})[prefix] = col
    return groups


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
        request (LayerConversionRequest): The pipeline request containing layer configurations and
            per-layer column mappings.
        tehsils (pl.LazyFrame): A lazy dataframe of tehsils.

    Returns:
        dict[str, pl.LazyFrame]: A dictionary mapping layer names to lazy Parquet glob scans.

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
        matching = list(Path(settings.temp_path).glob(f"{layer}_*.parquet"))
        if not matching:
            logger.error(
                f"No Parquet files found for layer '{layer}' — "
                "all worker tasks may have failed. Skipping this layer."
            )
            continue
        logger.info(f"Scanning {len(matching)} Parquet file(s) for layer '{layer}'")
        lazy_frames = [pl.scan_parquet(p) for p in matching]
        results[layer] = pl.concat(lazy_frames, how="diagonal_relaxed")

    return results


async def _fetch_version(s3_path: str) -> pl.LazyFrame:
    """Fetch the layer version metadata CSV from S3.

    Args:
        s3_path (str): The S3 path to the layer version CSV.

    Returns:
        pl.LazyFrame: A lazy dataframe containing the version metadata sorted by state.
    """
    df = pl.read_csv(s3_path)
    return (
        df.rename(
            {
                "State": "state_name",
                "District": "district_name",
                "Tehsil": "tehsil_name",
                "Algorithm Version": "version",
            }
        )
        .with_columns(pl.col("version").cast(pl.Float64, strict=False))
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
        base_layer (str): Path or URI to the base layer.
        super_layer_source (str | None): Optional super-layer boundary file path/URI.
        super_field (str | None): Column name in the super-layer to copy onto each row.

    Returns:
        pl.LazyFrame: A lazy dataframe of the base layer.

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
