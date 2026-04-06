import warnings

import polars as pl
import polars_st as st
from loguru import logger
from pyogrio.errors import DataSourceError

from src.conversion.helpers.cleaners import clean_label
from src.utils.configs import settings

warnings.filterwarnings("ignore", category=RuntimeWarning, module="pyogrio")


def merge_tehsils_on_layer(layer: str, tehsils: pl.LazyFrame) -> pl.LazyFrame:
    # using clean_tehsils here
    tehsil_data = tehsils.collect()

    frames = []
    for row in tehsil_data.iter_rows():
        tehsil_name = clean_label(row[4])
        district_name = clean_label(row[2])

        file_path = f"{settings.temp_path}{layer}_{district_name}_{tehsil_name}.geojson"
        try:
            df = st.read_file(file_path)
            if "geometry" not in df.columns:
                df = df.rename({"geom": "geometry"})
            frames.append(df.lazy())
        except DataSourceError:
            logger.error(f"File not found: {file_path}")
            continue

    return pl.concat(frames)
