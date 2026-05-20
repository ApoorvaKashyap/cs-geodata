import warnings

import polars as pl
import polars_st as st
from loguru import logger

from src.utils.configs import settings

warnings.filterwarnings("ignore", category=RuntimeWarning, module="pyogrio")
DROP_BEFORE_JOIN = ["id", "tehsil", "district", "state", "geometry"]


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
    base = base.join(location_meta, on="mws_id", how="left")

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

        # Deduplicate on mws_id alone — polygons spanning multiple tehsils
        # appear in multiple GeoJSON files.
        layer_df = layer_df.unique(subset=["mws_id"])

        merged = merged.join(
            layer_df,
            on="mws_id",
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
            c in schema for c in ["mws_id", "tehsil", "district", "state"]
        ):
            logger.info(f"Using '{layer_name}' as location metadata source")
            meta = layer_df.select(
                ["mws_id", "tehsil", "district", "state"]
            ).unique(subset=["mws_id"])

            count = meta.collect(engine="streaming").height
            logger.info(
                f"Location metadata: {count} unique mws_id pairs "
                f"from '{layer_name}'"
            )
            return meta

    raise ValueError(
        "No layer contains all of: mws_id, tehsil, district, state. "
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
            layer_df.select("mws_id").unique()
            for layer_df in layer_results.values()
            if "mws_id" in layer_df.collect_schema().names()
        ],
        how="diagonal_relaxed",
    ).unique()

    base_ids = base.select("mws_id")

    missing = base_ids.join(all_layer_ids, on="mws_id", how="anti")

    missing_count = missing.collect(engine="streaming").height
    logger.info(
        f"Found {missing_count} MWSv2 polygons not present in any active tehsil layer"
    )

    return missing
