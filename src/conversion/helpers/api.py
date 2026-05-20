import asyncio
import warnings
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import fsspec  # type: ignore[import-untyped]
import polars as pl
import polars_st as st  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
import pyogrio  # type: ignore[import-untyped]
import requests  # type: ignore[import-untyped]
from loguru import logger
from tqdm import tqdm  # type: ignore[import-untyped]

from src.conversion.helpers.cleaners import rename_and_drop
from src.utils.configs import settings

MWS_URL_MAPPING = {
    "soge": "https://geoserver.core-stack.org:8443/geoserver/soge/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=soge%3Asoge_vector_{district}_{tehsil}&outputFormat=application%2Fjson",
    "cropping_intensity": "https://geoserver.core-stack.org:8443/geoserver/crop_intensity/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=crop_intensity%3A{district}_{tehsil}_intensity&outputFormat=application%2Fjson",
    "terrain": "https://geoserver.core-stack.org:8443/geoserver/terrain/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=terrain%3A{district}_{tehsil}_cluster&outputFormat=application%2Fjson",
    "deltaG_fortnight": "https://geoserver.core-stack.org:8443/geoserver/mws_layers/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=mws_layers%3AdeltaG_fortnight_{district}_{tehsil}&outputFormat=application%2Fjson",
    "deltaG_well_depth": "https://geoserver.core-stack.org:8443/geoserver/mws_layers/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=mws_layers%3AdeltaG_well_depth_{district}_{tehsil}&outputFormat=application%2Fjson",
    "aquifer": "https://geoserver.core-stack.org:8443/geoserver/aquifer/ows?service=WFS&version=1.0.0&request=GetFeature&typeName=aquifer%3Aaquifer_vector_{district}_{tehsil}&outputFormat=application%2Fjson",
}


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
        layer: Layer name key present in MWS_URL_MAPPING.
        district: Slug-form district name used to build the GeoServer URL.
        tehsil: Slug-form tehsil name used to build the GeoServer URL.
        state_name: Human-readable state label to tag each row.
        district_name: Human-readable district label to tag each row.
        tehsil_name: Human-readable tehsil label to tag each row.
        cols_rename: Column rename mapping to apply after reading the file.
        cols_drop: Column names to drop after reading the file.

    Returns:
        0 on success, -1 on failure.
    """
    url = MWS_URL_MAPPING[layer].format(district=district, tehsil=tehsil)
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
    """Asynchronously convert a base layer to Parquet format.

    Args:
        input_path: Path or URI of the input file.
        output_path: Destination path for the converted Parquet file.
        chunk_size: Number of rows to process per chunk. Defaults to 500000.

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
            pool, _convert_base_sync, input_path, output_path, chunk_size
        )


def _convert_base_sync(input_path: str, output_path: str, chunk_size: int) -> bool:
    """Synchronously process the base layer conversion using pyogrio.

    Args:
        input_path: Path or URI of the input file.
        output_path: Destination path for the converted Parquet file.
        chunk_size: Number of rows to process per chunk.

    Returns:
        True if conversion succeeded, False otherwise.
    """
    warnings.filterwarnings("ignore", category=RuntimeWarning, module="pyogrio")

    try:
        input_fs, _ = fsspec.core.url_to_fs(input_path)
        if not input_fs.exists(input_path):
            logger.error(f"Input file not found: {input_path}")
            return False
    except Exception as e:
        logger.error(f"Error accessing input path: {str(e)}")
        return False

    # Get total row count for progress bar
    try:
        info = pyogrio.read_info(input_path)
        total_features = info["features"]
    except Exception:
        total_features = None  # fallback to unknown total

    logger.info(f"Starting conversion: {input_path} -> {output_path}")
    total_rows = 0

    try:
        reader = pyogrio.read_dataframe(input_path, chunksize=chunk_size)

        with fsspec.open(output_path, mode="wb") as output_handle:
            writer = None
            try:
                with tqdm(
                    total=total_features,
                    unit="rows",
                    desc=f"Converting {Path(input_path).name}",
                    dynamic_ncols=True,
                ) as pbar:
                    for i, gdf_chunk in enumerate(reader):
                        batch = pl.from_pandas(gdf_chunk)

                        if "geometry" not in batch.columns and "geom" in batch.columns:
                            batch = batch.rename({"geom": "geometry"})

                        table = batch.to_arrow()

                        if writer is None:
                            writer = pq.ParquetWriter(
                                output_handle, table.schema, compression="snappy"
                            )

                        writer.write_table(table)
                        total_rows += len(batch)
                        pbar.update(len(batch))
                        pbar.set_postfix(batch=i + 1, rows=total_rows)

            finally:
                if writer is not None:
                    writer.close()

        if total_rows == 0:
            logger.error("No rows were written.")
            return False

        return True

    except Exception as e:
        logger.error(f"Conversion failed for {input_path}: {str(e)}")
        try:
            fs, _ = fsspec.core.url_to_fs(output_path)
            if fs.exists(output_path):
                fs.rm(output_path)
        except Exception:
            pass
        return False
