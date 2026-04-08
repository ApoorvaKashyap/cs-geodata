import warnings

import polars as pl
import polars_st as st
from loguru import logger
from pyogrio.errors import DataSourceError
from tqdm import tqdm

from src.conversion.helpers.cleaners import clean_label, rename_and_drop
from src.utils.configs import settings

warnings.filterwarnings("ignore", category=RuntimeWarning, module="pyogrio")


# Check id, version before joining
def merge_tehsils_on_layer(
    layer: str,
    tehsils: pl.LazyFrame,
    cols_rename: dict[str, str],
    drop_cols: list[str],
) -> pl.LazyFrame:
    tehsil_data = tehsils.collect()
    frames: list[pl.LazyFrame] = []

    for row in tqdm(tehsil_data.to_dicts()):
        tehsil_name = clean_label(row.get("tehsil_name", ""))
        district_name = clean_label(row.get("district_name", ""))

        file_path = f"{settings.temp_path}{layer}_{district_name}_{tehsil_name}.geojson"

        try:
            df = st.read_file(file_path)
            df = rename_and_drop(df.lazy(), cols_rename, drop_cols).collect()
            if "geometry" not in df.columns and "geom" in df.columns:
                df = st.GeoDataFrame(df.rename({"geom": "geometry"}))
            if df.is_empty():
                continue
            print(tehsils.head(5))
            exprs = []
            for col_target, col_source in [
                ("version", "algorithm version"),
                ("tehsil", "tehsil_name"),
                ("district", "district_name"),
                ("state", "state_name"),
            ]:
                val = row.get(col_source)
                if col_target == "version":
                    val_expr = pl.lit(val).cast(pl.Float64, strict=False)
                else:
                    val_expr = pl.lit(val)

                exprs.append(val_expr.alias(col_target))

            if exprs:
                df = df.with_columns(exprs)

            frames.append(df.lazy())
        except DataSourceError:
            logger.error(f"File not found: {file_path}")
            continue

    if not frames:
        raise ValueError(f"No files found for layer: {layer}")

    merged = pl.concat(frames, how="diagonal_relaxed")

    return merged.lazy()
