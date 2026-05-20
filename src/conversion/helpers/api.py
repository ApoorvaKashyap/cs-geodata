import asyncio
import contextlib
import warnings
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import fsspec  # type: ignore[import-untyped]
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
        A polars DataFrame containing the active locations JSON response.
    """
    response = requests.get(
        f"{settings.corestack_api_url}/get_active_locations/",
        headers={"X-API-KEY": f"{settings.corestack_api_key.get_secret_value()}"},
    )
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
) -> int:
    """Download a GeoJSON from GeoServer, clean it, and persist as Parquet.

    Combines the former download-only step with the per-tehsil column cleaning
    and admin-boundary tagging that was previously performed on the main thread
    inside ``merge_tehsils_on_layer``.  Running this inside an RQ worker frees
    the pipeline orchestrator from the CPU-heavy GDAL GeoJSON parse.

    The output Parquet file is written to
    ``{settings.temp_path}/{layer}_{district}_{tehsil}.parquet``.

    Args:
        layer: Layer name (used for the output filename).
        district: Slug-form district name used to build the GeoServer URL.
        tehsil: Slug-form tehsil name used to build the GeoServer URL.
        state_name: Human-readable state label to tag each row.
        district_name: Human-readable district label to tag each row.
        tehsil_name: Human-readable tehsil label to tag each row.
        url_template: WFS URL template with {district} and {tehsil} placeholders.
        cols_rename: Column rename mapping to apply after reading the file.
        cols_drop: Column names to drop after reading the file.

    Returns:
        0 on success, -1 on failure.
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
            logger.warning(
                f"Empty GeoJSON for {layer}/{district}/{tehsil} — skipping"
            )
            return 0

        df = rename_and_drop(
            df.lazy(), cols_rename, cols_drop
        ).collect(engine="streaming")

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

        df.write_parquet(parquet_path, compression="zstd")
        logger.info(f"Written {parquet_path} ({df.height} rows)")
        return 0

    except Exception as exc:
        logger.error(f"Failed to convert {layer}/{district}/{tehsil}: {exc}")
        return -1

    finally:
        Path(geojson_path).unlink(missing_ok=True)


async def convert_base(
    input_path: str, output_path: str, chunk_size: int = 500000
) -> bool:
    """Asynchronously convert a base layer to Parquet format, sorted by Hilbert curve.

    Reads any OGR-supported format (local file, S3 URI, HTTPS URL) using
    DuckDB's ``ST_Read``, sorts rows spatially via ``ST_Hilbert`` so that
    geographically nearby polygons are physically adjacent in the file, and
    writes a compressed Parquet with a fixed row-group size.

    Sorting at conversion time means all downstream joins and scans on the
    base layer benefit from spatial locality without any extra work later.

    Args:
        input_path: Path or URI of the input file (local, ``s3://``, HTTPS).
        output_path: Destination path for the converted Parquet file.
        chunk_size: Unused; kept for API compatibility.

    Returns:
        True if conversion succeeded, False otherwise.
    """
    if input_path == output_path:
        logger.error(
            "Input and output paths are identical. This would truncate the source file."
        )
        output_path = str(Path(output_path).with_suffix(".converted.parquet"))

    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        return await loop.run_in_executor(
            pool, _convert_base_sync, input_path, output_path
        )


def _convert_base_sync(input_path: str, output_path: str) -> bool:
    """Synchronously convert and Hilbert-sort a base layer using DuckDB.

    Uses DuckDB's spatial extension to read any OGR-supported source
    (including ``s3://`` URIs via the credential chain), sort rows by
    ``ST_Hilbert(geom)``, and write a single Parquet file.

    The geometry column is kept as ``geom`` (WKB binary) so that the
    existing pipeline rename ``geom -> geometry`` in ``run_mws_pipeline``
    continues to work unchanged.

    Args:
        input_path: Path or URI of the input file.
        output_path: Destination path for the output Parquet file.

    Returns:
        True if conversion succeeded, False otherwise.
    """
    logger.info(f"Starting Hilbert-sorted conversion: {input_path} -> {output_path}")

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    conn = init_duckdb()
    try:
        sql = f"""
            COPY (
                SELECT
                    * EXCLUDE (geom),
                    ST_AsWKB(geom) AS geom
                FROM ST_Read('{input_path}')
                ORDER BY ST_Hilbert(geom)
            )
            TO '{output_path}'
            WITH (
                FORMAT 'PARQUET',
                COMPRESSION 'ZSTD',
                ROW_GROUP_SIZE 100000
            );
        """
        logger.debug(f"Executing:\n{sql}")
        conn.execute(sql)

        logger.info(f"Hilbert-sorted base layer written to {output_path}")
        return True

    except Exception as exc:
        logger.error(f"Conversion failed for {input_path}: {exc}")
        with contextlib.suppress(Exception):
            Path(output_path).unlink(missing_ok=True)
        return False

    finally:
        conn.close()
        import glob

        for f in glob.glob("/tmp/duckdb_*.db"):
            with contextlib.suppress(Exception):
                Path(f).unlink()
