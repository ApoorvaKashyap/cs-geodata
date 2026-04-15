import asyncio
import warnings
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import fsspec
import polars as pl
import pyarrow.parquet as pq
import pyogrio
import requests
from loguru import logger
from tqdm import tqdm

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
    response = requests.get(
        f"{settings.corestack_api_url}/get_active_locations/",
        headers={"X-API-KEY": f"{settings.corestack_api_key.get_secret_value()}"},
    )
    _df = pl.read_json(response.content)
    return _df


async def get_geojson(layer: str, district: str, tehsil: str) -> int:
    url = MWS_URL_MAPPING[layer].format(district=district, tehsil=tehsil)
    logger.info(
        f"Fetching geojson for layer {layer} and district {district} and tehsil {tehsil}"
        f"using url {url}"
    )
    response = requests.get(url)
    if response.status_code == 200:
        try:
            with open(
                f"{settings.temp_path}/{layer}_{district}_{tehsil}.geojson", "w"
            ) as f:
                f.write(response.text)
            return 0
        except Exception as e:
            logger.error(
                f"Failed to write geojson for {layer} {district} {tehsil}: {e}"
            )
    return -1


async def convert_base(
    input_path: str, output_path: str, chunk_size: int = 500000
) -> bool:
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
