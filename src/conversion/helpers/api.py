import asyncio
import contextlib
import warnings
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl
import polars_st as st  # type: ignore[import-untyped]
import requests  # type: ignore[import-untyped]
from loguru import logger

from src.conversion.helpers.cleaners import rename_and_drop
from src.conversion.helpers.duckdb_funcs import init_duckdb
from src.utils.configs import settings


async def get_active() -> pl.DataFrame:
    """Fetch the list of active locations from the CoreStack API.

    Returns:
        pl.DataFrame: A polars DataFrame containing the active locations JSON response.
    """
    response = requests.get(
        f"{settings.corestack_api_url}/get_active_locations/",
        headers={"X-API-KEY": f"{settings.corestack_api_key.get_secret_value()}"},
    )
    response.raise_for_status()
    _df = pl.read_json(response.content)
    return _df


def download_and_convert_geojson(
    layer: str,
    district: str,
    tehsil: str,
    state_name: str,
    district_name: str,
    tehsil_name: str,
    url_template: str,
    cols_rename: dict[str, str],
    cols_drop: list[str],
    m2_to_ha: list[str],
) -> int:
    """Download a GeoJSON from GeoServer, clean it, and persist as Parquet.

    Combines the former download-only step with the per-tehsil column cleaning
    and admin-boundary tagging that was previously performed on the main thread
    inside ``merge_tehsils_on_layer``. Running this inside an RQ worker frees
    the pipeline orchestrator from the CPU-heavy GDAL GeoJSON parse.

    The output Parquet file is written to
    ``{settings.temp_path}/{layer}_{district}_{tehsil}.parquet``.

    Args:
        layer (str): Layer name (used for the output filename).
        district (str): Slug-form district name used to build the GeoServer URL.
        tehsil (str): Slug-form tehsil name used to build the GeoServer URL.
        state_name (str): Human-readable state label to tag each row.
        district_name (str): Human-readable district label to tag each row.
        tehsil_name (str): Human-readable tehsil label to tag each row.
        url_template (str): WFS URL template with {district} and {tehsil} placeholders.
        cols_rename (dict[str, str]): Column rename mapping to apply after reading the file.
        cols_drop (list[str]): Column names to drop after reading the file.

    Returns:
        int: 0 on success, -1 on failure.
    """
    url = url_template.format(district=district, tehsil=tehsil)
    logger.info(
        f"Fetching + converting layer={layer} district={district} tehsil={tehsil}"
    )

    response = requests.get(url)
    if response.status_code != 200:
        logger.warning(
            f"HTTP {response.status_code} for {layer}/{district}/{tehsil} — skipping"
        )
        return -1

    geojson_path = f"{settings.temp_path}/{layer}_{district}_{tehsil}.geojson"
    parquet_path = f"{settings.temp_path}/{layer}_{district}_{tehsil}.parquet"

    try:
        with open(geojson_path, "w") as fh:
            fh.write(response.text)

        warnings.filterwarnings("ignore", category=RuntimeWarning, module="pyogrio")
        df = st.read_file(geojson_path)

        if df.is_empty():
            logger.warning(f"Empty GeoJSON for {layer}/{district}/{tehsil} — skipping")
            return 0

        df = rename_and_drop(df.lazy(), cols_rename, cols_drop)

        from src.conversion.helpers.cleaners import convert_m2_to_ha

        df = convert_m2_to_ha(df, m2_to_ha).collect(engine="streaming")

        if "geometry" not in df.columns and "geom" in df.columns:
            df = df.rename({"geom": "geometry"})

        # Tag every row with its admin boundary metadata.
        # (Previously done on the main thread inside merge_tehsils_on_layer.)
        df = df.with_columns(
            [
                pl.lit(state_name).alias("state"),
                pl.lit(district_name).alias("district"),
                pl.lit(tehsil_name).alias("tehsil"),
            ]
        )

        df.write_parquet(
            parquet_path,
            compression="zstd",
            compression_level=settings.parquet_compression_level,
            row_group_size=settings.parquet_row_group_size,
        )
        logger.info(f"Written {parquet_path} ({df.height} rows)")
        return 0

    except Exception as exc:
        logger.error(f"Failed to convert {layer}/{district}/{tehsil}: {exc}")
        return -1

    finally:
        Path(geojson_path).unlink(missing_ok=True)


async def convert_base(
    input_path: str,
    output_path: str,
    chunk_size: int = 500000,
    super_layer_source: str | None = None,
    super_field: str | None = None,
) -> bool:
    """Asynchronously convert a base layer to Parquet format, sorted by Hilbert curve.

    If *super_layer_source* and *super_field* are provided, each base entity is also
    assigned its containing super-layer region (e.g. sub-basin) via a
    centroid-in-polygon spatial join, enabling downstream Hive partitioning.

    Args:
        input_path (str): Path or URI of the input file (local, ``s3://``, HTTPS).
        output_path (str): Destination path for the converted Parquet file.
        chunk_size (int): Unused; kept for API compatibility.
        super_layer_source (str | None): Optional path/URI to the super-layer boundary file.
        super_field (str | None): Column name in the super-layer file to copy onto each row.

    Returns:
        bool: True if conversion succeeded, False otherwise.
    """
    if input_path == output_path:
        logger.error(
            "Input and output paths are identical. This would truncate the source file."
        )
        output_path = str(Path(output_path).with_suffix(".converted.parquet"))

    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        return await loop.run_in_executor(
            pool,
            _convert_base_sync,
            input_path,
            output_path,
            super_layer_source,
            super_field,
        )


def _convert_base_sync(
    input_path: str,
    output_path: str,
    super_layer_source: str | None = None,
    super_field: str | None = None,
) -> bool:
    """Synchronously convert and Hilbert-sort a base layer using DuckDB.

    When *super_layer_source* and *super_field* are both provided the
    conversion runs in **two steps**:

    1. ``ST_Read`` the source → ``ORDER BY ST_Hilbert(geom)`` → local temp Parquet.
    2. ``parquet_scan(tmp)`` LEFT JOIN super-layer on centroid-in-polygon →
       final Parquet at *output_path* (may be an ``s3://`` URI).

    The temp file is always deleted in the ``finally`` block.

    When no super-layer is provided the function falls back to the original
    single-step Hilbert-sort COPY.

    The geometry column is kept as ``geom`` (WKB binary) so that the
    existing pipeline rename ``geom -> geometry`` in ``run_mws_pipeline``
    continues to work unchanged.

    Args:
        input_path (str): Path or URI of the input file.
        output_path (str): Destination path for the output Parquet file.
        super_layer_source (str | None): Optional path/URI to the super-layer boundary file.
        super_field (str | None): Column name in the super-layer whose value is copied to each row.

    Returns:
        bool: True if conversion succeeded, False otherwise.
    """
    import tempfile

    logger.info(f"Starting Hilbert-sorted conversion: {input_path} -> {output_path}")
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    tmp_path: str | None = None
    conn = init_duckdb()
    try:
        if super_layer_source and super_field:
            # Step 1: Hilbert-sort to a local temp parquet
            tmp_fd, tmp_path = tempfile.mkstemp(suffix="_base_hilbert.parquet")
            import os

            os.close(tmp_fd)

            logger.info(f"Step 1: Hilbert sort -> temp {tmp_path}")
            conn.execute(f"""
                COPY (
                    SELECT
                        * EXCLUDE (geom),
                        ST_AsWKB(geom) AS geom
                    FROM ST_Read('{input_path}')
                    ORDER BY ST_Hilbert(geom)
                )
                TO '{tmp_path}'
                WITH (FORMAT 'PARQUET', COMPRESSION 'ZSTD', COMPRESSION_LEVEL {settings.parquet_compression_level}, ROW_GROUP_SIZE {settings.parquet_row_group_size});
            """)

            # Step 2: Spatial join with super layer → final output
            logger.info(
                f"Step 2: Spatial join with super layer "
                f"({super_layer_source}) on field '{super_field}'"
            )
            conn.execute(f"""
                CREATE TABLE _super AS
                SELECT
                    geom AS _poly,
                    {super_field}
                FROM ST_Read('{super_layer_source}');
            """)
            conn.execute(f"""
                COPY (
                    SELECT
                        b.* EXCLUDE (_geom),
                        s.{super_field}
                    FROM (
                        SELECT *, ST_GeomFromWKB(geom) AS _geom
                        FROM parquet_scan('{tmp_path}')
                    ) b
                    LEFT JOIN _super s
                        ON ST_Within(ST_Centroid(b._geom), s._poly)
                )
                TO '{output_path}'
                WITH (FORMAT 'PARQUET', COMPRESSION 'ZSTD', COMPRESSION_LEVEL {settings.parquet_compression_level}, ROW_GROUP_SIZE {settings.parquet_row_group_size});
            """)
        else:
            # Single-step: Hilbert sort only (no super layer)
            conn.execute(f"""
                COPY (
                    SELECT
                        * EXCLUDE (geom),
                        ST_AsWKB(geom) AS geom
                    FROM ST_Read('{input_path}')
                    ORDER BY ST_Hilbert(geom)
                )
                TO '{output_path}'
                WITH (FORMAT 'PARQUET', COMPRESSION 'ZSTD', COMPRESSION_LEVEL {settings.parquet_compression_level}, ROW_GROUP_SIZE {settings.parquet_row_group_size});
            """)

        logger.info(f"Base layer written to {output_path}")
        return True

    except Exception as exc:
        logger.error(f"Conversion failed for {input_path}: {exc}")
        with contextlib.suppress(Exception):
            Path(output_path).unlink(missing_ok=True)
        return False

    finally:
        conn.close()
        if tmp_path:
            with contextlib.suppress(Exception):
                Path(tmp_path).unlink(missing_ok=True)
        import glob

        for f in glob.glob("/tmp/duckdb_*.db"):
            with contextlib.suppress(Exception):
                Path(f).unlink()
