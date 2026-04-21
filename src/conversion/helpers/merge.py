import warnings

import polars as pl
import polars_st as st
from loguru import logger
from pyogrio.errors import DataSourceError  # type: ignore[import-untyped]
from tqdm import tqdm  # type: ignore[import-untyped]

from src.conversion.helpers.cleaners import clean_label, rename_and_drop
from src.utils.configs import settings

warnings.filterwarnings("ignore", category=RuntimeWarning, module="pyogrio")
DROP_BEFORE_JOIN = ["id", "tehsil", "district", "state", "geometry"]


def merge_tehsils_on_layer(
    layer: str,
    tehsils: pl.LazyFrame,
    cols_rename: dict[str, str],
    drop_cols: list[str],
) -> pl.LazyFrame:
    """Merge all tehsil-level GeoJSONs for a specific layer.

    Args:
        layer: Name of the layer being processed.
        tehsils: Dataframe containing active tehsil metadata (name, district, state).
        cols_rename: Dictionary mapping original column names to target names.
        drop_cols: List of columns to drop from the individual GeoJSONs.

    Returns:
        A concatenated LazyFrame containing data from all found tehsil files.

    Raises:
        ValueError: If no valid GeoJSON files were found for the layer.
    """
    tehsil_data = tehsils.collect(engine="streaming")
    frames: list[pl.LazyFrame] = []

    for row in tqdm(tehsil_data.to_dicts()):
        tehsil_name = clean_label(row.get("tehsil_name", ""))
        district_name = clean_label(row.get("district_name", ""))

        file_path = f"{settings.temp_path}{layer}_{district_name}_{tehsil_name}.geojson"

        try:
            df = st.read_file(file_path)
            df: pl.DataFrame = rename_and_drop(
                df.lazy(), cols_rename, drop_cols
            ).collect(engine="streaming")
            if "geometry" not in df.columns and "geom" in df.columns:
                df = pl.DataFrame(df.rename({"geom": "geometry"}))
            if df.is_empty():
                continue
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
                df = pl.DataFrame(df.with_columns(exprs))

            frames.append(df.lazy())
        except DataSourceError:
            logger.error(f"File not found: {file_path}")
            continue

    if not frames:
        raise ValueError(f"No files found for layer: {layer}")

    merged = pl.concat(frames, how="diagonal_relaxed")

    return merged.lazy()


def merge_all_layers(
    layer_results: dict[str, pl.LazyFrame],
    base: pl.LazyFrame,
) -> pl.LazyFrame:
    """Merge all processed layer dataframes onto the base dataset.

    Base must arrive already renamed (uid->mws_id, geom->geometry)
    and versioned (version=1.2). This is the caller's responsibility.

    Args:
        layer_results: Dictionary of layer names mapped to their LazyFrames.
        base: The base MWS LazyFrame.

    Returns:
        The fully merged LazyFrame containing all layers joined on mws_id and version.

    Raises:
        ValueError: If the base layer is missing expected columns.
    """
    # Validate base has expected columns
    base_schema = base.collect_schema().names()
    for col in ["mws_id", "geometry", "area_in_ha", "version"]:
        if col not in base_schema:
            raise ValueError(f"Base layer missing expected column: '{col}'")

    logger.info("Extracting location metadata from layers")
    location_meta = _extract_location_meta(layer_results)
    base = base.join(location_meta, on=["mws_id", "version"], how="left")

    merged = base
    for layer_name, layer_df in layer_results.items():
        logger.info(f"Joining layer: {layer_name}")

        cols_to_drop = [
            c for c in DROP_BEFORE_JOIN if c in layer_df.collect_schema().names()
        ]
        layer_df = layer_df.drop(cols_to_drop, strict=False)

        if "area_in_ha" in layer_df.collect_schema().names():
            logger.warning(
                f"Dropping area_in_ha from {layer_name} — MWSv2 is authoritative"
            )
            layer_df = layer_df.drop("area_in_ha")

        # Deduplicate: polygons spanning multiple tehsils appear in
        # multiple GeoJSON files. Without this, sequential left joins
        # fan out multiplicatively (2^N_layers duplicates per polygon).
        layer_df = layer_df.unique(subset=["mws_id", "version"])

        merged = merged.join(
            layer_df,
            on=["mws_id", "version"],
            how="left",
            suffix=f"_{layer_name}",
        )

        logger.info(
            f"Joined '{layer_name}' — "
            f"schema width: {len(merged.collect_schema().names())} columns"
        )

    return merged


def _extract_location_meta(
    layer_results: dict[str, pl.LazyFrame],
) -> pl.LazyFrame:
    """Extract authoritative location metadata from the layers.

    Retrieves mws_id + version + tehsil + district + state from layers.
    Only covers MWS polygons that were in active tehsils. Polygons outside
    active tehsils will correctly get null location metadata after the left join.

    Prefers simpler layers (terrain, soge) with lowest chance of null identity cols.
    Deduplicates on mws_id + version in case a polygon appears in multiple tehsil files.

    Args:
        layer_results: Dictionary mapping layer names to their LazyFrames.

    Returns:
        A LazyFrame containing unique location mappings for mws_ids.

    Raises:
        ValueError: If no single layer contains all necessary location columns.
    """
    preferred_order = [
        "terrain",
        "soge",
        "aquifer",
        "cropping_intensity",
        "deltaG_fortnight",
        "deltaG_well_depth",
    ]
    ordered = preferred_order + [
        layer_key for layer_key in layer_results if layer_key not in preferred_order
    ]

    for layer_name in ordered:
        if layer_name not in layer_results:
            continue

        layer_df = layer_results[layer_name]
        schema = layer_df.collect_schema().names()

        if all(
            c in schema for c in ["mws_id", "version", "tehsil", "district", "state"]
        ):
            logger.info(f"Using '{layer_name}' as location metadata source")
            meta = layer_df.select(
                ["mws_id", "version", "tehsil", "district", "state"]
            ).unique(subset=["mws_id", "version"])

            # Warn if there are duplicate mws_id+version after dedup
            # (shouldn't happen but indicates upstream data issues)
            count = meta.collect(engine="streaming").height
            logger.info(
                f"Location metadata: {count} unique mws_id+version pairs "
                f"from '{layer_name}'"
            )
            return meta

    raise ValueError(
        "No layer contains all of: mws_id, version, tehsil, district, state. "
        "Cannot extract location metadata."
    )


def _get_missing_mws_ids(
    base: pl.LazyFrame,
    layer_results: dict[str, pl.LazyFrame],
) -> pl.LazyFrame:
    """Identify MWSv2 polygon IDs that do not appear in any layer.

    These are typically polygons outside the active tehsil list.

    Args:
        base: The base MWS LazyFrame.
        layer_results: Dictionary mapping layer names to their LazyFrames.

    Returns:
        A LazyFrame containing the missing mws_id and version pairs.
    """
    all_layer_ids = pl.concat(
        [
            layer_df.select(["mws_id", "version"]).unique()
            for layer_df in layer_results.values()
            if "mws_id" in layer_df.collect_schema().names()
        ],
        how="diagonal_relaxed",
    ).unique()

    base_ids = base.select(["mws_id", "version"])

    missing = base_ids.join(all_layer_ids, on=["mws_id", "version"], how="anti")

    missing_count = missing.collect(engine="streaming").height
    logger.info(
        f"Found {missing_count} MWSv2 polygons not present in any active tehsil layer"
    )

    return missing
