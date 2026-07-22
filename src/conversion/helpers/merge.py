"""Algorithms for merging base and attribute layers."""

import warnings

import polars as pl
from loguru import logger


warnings.filterwarnings("ignore", category=RuntimeWarning, module="pyogrio")
DROP_BEFORE_JOIN = ["id", "tehsil", "district", "state", "geometry"]


def merge_all_layers(
    layer_results: dict[str, pl.LazyFrame],
    base: pl.LazyFrame,
    entity_key: str = "mws_id",
) -> pl.LazyFrame:
    """Merge all processed layer dataframes onto the base dataset.

    Base must arrive already renamed (uid->entity_key, geom->geometry)
    and versioned. This is the caller's responsibility.

    Args:
        layer_results (dict[str, pl.LazyFrame]): Dictionary of layer names mapped to their LazyFrames.
        base (pl.LazyFrame): The base entity LazyFrame.
        entity_key (str): The join key column name shared across all layers (default: 'mws_id').

    Returns:
        pl.LazyFrame: The fully merged LazyFrame containing all layers joined on entity_key.

    Raises:
        ValueError: If the base layer is missing expected columns.

    """
    # Validate base has expected columns
    base_schema = base.collect_schema().names()
    if entity_key not in base_schema:
        raise ValueError(f"Base layer missing entity key column: '{entity_key}'")
    if "geometry" not in base_schema:
        raise ValueError("Base layer missing expected column: 'geometry'")
    if "area_in_ha" not in base_schema:
        logger.warning(
            "Base layer missing 'area_in_ha' column — proceeding without it."
        )

    # Force join keys to string to prevent datatype mismatches (e.g. i32 vs str)
    base = base.with_columns(pl.col(entity_key).cast(pl.Utf8))
    for layer_name, layer_df in layer_results.items():
        if entity_key in layer_df.collect_schema().names():
            layer_results[layer_name] = layer_df.with_columns(
                pl.col(entity_key).cast(pl.Utf8)
            )

    logger.info("Extracting location metadata from layers")
    location_meta = _extract_location_meta(layer_results, entity_key=entity_key)
    base = base.join(location_meta, on=entity_key, how="left")

    merged = base
    for layer_name, layer_df in layer_results.items():
        logger.info(f"Joining layer: {layer_name}")

        cols_to_drop = [
            c for c in DROP_BEFORE_JOIN if c in layer_df.collect_schema().names()
        ]
        layer_df = layer_df.drop(cols_to_drop, strict=False)

        if "area_in_ha" in layer_df.collect_schema().names():
            logger.warning(
                f"Dropping area_in_ha from {layer_name} — base layer is authoritative"
            )
            layer_df = layer_df.drop("area_in_ha")

        # Deduplicate on entity_key alone — polygons spanning multiple tehsils
        # appear in multiple GeoJSON files.
        # Prefer non-null values when duplicates exist
        layer_df = (
            layer_df.sort(by=entity_key)
            .group_by(entity_key)
            .agg(pl.all().drop_nulls().first())
        )

        # Validate entity_key exists before joining
        if entity_key not in layer_df.collect_schema().names():
            raise ValueError(
                f"Layer '{layer_name}' missing '{entity_key}' column after processing. "
                f"Check descriptor rename mapping. Available columns: {layer_df.collect_schema().names()}"
            )

        merged = merged.join(
            layer_df,
            on=entity_key,
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
    entity_key: str = "mws_id",
) -> pl.LazyFrame:
    """Extract authoritative location metadata from the layers.

    Retrieves entity_key + tehsil + district + state from layers.
    Only covers entity polygons that were in active tehsils. Polygons outside
    active tehsils will correctly get null location metadata after the left join.

    Prefers simpler layers (terrain, soge) with lowest chance of null identity cols.
    Deduplicates on entity_key in case a polygon appears in multiple tehsil files.

    Args:
        layer_results (dict[str, pl.LazyFrame]): Dictionary mapping layer names to their LazyFrames.
        entity_key (str): The join key column name (default: 'mws_id').

    Returns:
        pl.LazyFrame: A LazyFrame containing unique location mappings for entity IDs.

    Raises:
        ValueError: If no single layer contains all necessary location columns.

    """
    if not layer_results:
        logger.info(
            "No attribute layers available — skipping location metadata extraction."
        )
        return pl.LazyFrame(
            schema={
                entity_key: pl.Utf8,
                "tehsil": pl.Utf8,
                "district": pl.Utf8,
                "state": pl.Utf8,
            }
        )

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

    meta_frames = []
    for layer_name in ordered:
        if layer_name not in layer_results:
            continue

        layer_df = layer_results[layer_name]
        schema = layer_df.collect_schema().names()

        if all(c in schema for c in [entity_key, "tehsil", "district", "state"]):
            meta_frames.append(
                layer_df.select([entity_key, "tehsil", "district", "state"])
            )

    if not meta_frames:
        raise ValueError(
            f"No layer contains all of: {entity_key}, tehsil, district, state. "
            "Cannot extract location metadata."
        )

    logger.info(f"Combining location metadata from {len(meta_frames)} layer(s)")
    meta = (
        pl.concat(meta_frames)
        .drop_nulls(subset=[entity_key])
        .unique(subset=[entity_key], keep="first")
    )

    count = meta.select(pl.len()).collect()[0, 0]
    logger.info(
        f"Location metadata: {count} unique {entity_key} pairs extracted globally"
    )
    return meta


def _get_missing_entity_ids(
    base: pl.LazyFrame,
    layer_results: dict[str, pl.LazyFrame],
    entity_key: str = "mws_id",
) -> pl.LazyFrame:
    """Identify entity polygon IDs that do not appear in any layer.

    These are typically polygons outside the active tehsil list.

    Args:
        base (pl.LazyFrame): The base entity LazyFrame.
        layer_results (dict[str, pl.LazyFrame]): Dictionary mapping layer names to their LazyFrames.
        entity_key (str): The join key column name (default: 'mws_id').

    Returns:
        pl.LazyFrame: A LazyFrame containing the missing entity IDs.

    """
    if not layer_results:
        logger.info("No attribute layers — returning empty missing-ID frame.")
        return pl.LazyFrame(schema={entity_key: pl.Utf8})

    all_layer_ids = pl.concat(
        [
            layer_df.select(entity_key).unique()
            for layer_df in layer_results.values()
            if entity_key in layer_df.collect_schema().names()
        ],
        how="diagonal_relaxed",
    ).unique()

    base_ids = base.select(entity_key)

    missing = base_ids.join(all_layer_ids, on=entity_key, how="anti")

    missing_count = missing.collect(engine="streaming").height
    logger.info(
        f"Found {missing_count} entity polygons not present in any active tehsil layer"
    )

    return missing
