import sys
from pathlib import Path

import polars as pl
import polars_st as st
from loguru import logger


def check_geoparquet(output_path: str) -> None:
    logger.info(f"Checking GeoParquet dataset at {output_path}")

    # Scan all partitions
    df = pl.scan_parquet(
        f"{output_path}/**/*.parquet",
        hive_partitioning=True,
    )

    schema = df.collect_schema()
    logger.info(f"Total columns: {len(schema.names())}")
    logger.info(f"First 20 columns: {schema.names()[:20]}")

    # ── Row counts ────────────────────────────────────────────────────────────
    total = df.select(pl.len()).collect().item()
    logger.info(f"Total rows: {total:,}")

    if total == 0:
        logger.error("Dataset is empty — aborting checks")
        return

    # ── Partition file inventory ───────────────────────────────────────────────
    partition_files = list(Path(output_path).rglob("*.parquet"))
    empty_files = [f for f in partition_files if f.stat().st_size == 0]
    logger.info(f"Partition files: {len(partition_files)}")
    if empty_files:
        logger.warning(f"Empty partition files ({len(empty_files)}): {empty_files}")

    # ── Rows per state ────────────────────────────────────────────────────────
    per_state = (
        df.group_by("state")
        .agg(pl.len().alias("row_count"))
        .sort("row_count", descending=True)
        .collect()
    )
    logger.info(f"Rows per state:\n{per_state}")

    # ── Null counts on key columns ─────────────────────────────────────────────
    key_cols = [
        "mws_id",
        "version",
        "geometry",
        "state",
        "district",
        "tehsil",
        "area_in_ha",
        "bbox",
    ]
    present_key_cols = [c for c in key_cols if c in schema.names()]
    missing_key_cols = [c for c in key_cols if c not in schema.names()]

    if missing_key_cols:
        logger.warning(f"Missing key columns: {missing_key_cols}")

    null_counts = df.select(
        [pl.col(c).is_null().sum().alias(c) for c in present_key_cols]
    ).collect()
    logger.info(f"Null counts for key columns:\n{null_counts}")

    # ── Duplicate composite key ───────────────────────────────────────────────
    dupes = (
        df.select(["mws_id", "version"])
        .collect(engine="streaming")
        .group_by(["mws_id", "version"])
        .len()
        .filter(pl.col("len") > 1)
    )
    if dupes.height > 0:
        logger.warning(f"Duplicate mws_id+version pairs: {dupes.height}")
        logger.warning(f"Sample duplicates:\n{dupes.head(5)}")
    else:
        logger.info("No duplicate mws_id+version pairs ✓")

    # ── Version distribution ───────────────────────────────────────────────────
    versions = df.group_by("version").agg(pl.len().alias("count")).collect()
    logger.info(f"Version distribution:\n{versions}")

    # ── Geometry validity ─────────────────────────────────────────────────────
    geo_df = df.select("geometry").collect(engine="streaming")

    invalid_geom = geo_df.with_columns(
        st.geom("geometry").st.is_valid().alias("is_valid")  # type: ignore[attr-defined]
    ).filter(pl.col("is_valid").not_())
    if invalid_geom.height > 0:
        logger.warning(f"Invalid geometries: {invalid_geom.height}")
    else:
        logger.info("All geometries valid ✓")

    # ── Geometry bounds ───────────────────────────────────────────────────────
    bounds_df = geo_df.with_columns(
        st.geom("geometry").st.bounds().alias("bounds")  # type: ignore[attr-defined]
    )
    global_bbox = [
        bounds_df["bounds"].arr.get(0).min(),  # xmin
        bounds_df["bounds"].arr.get(1).min(),  # ymin
        bounds_df["bounds"].arr.get(2).max(),  # xmax
        bounds_df["bounds"].arr.get(3).max(),  # ymax
    ]
    logger.info("Actual geometry bounds (should be within India 68-97°E, 6-37°N):")
    logger.info(f"  xmin (lon): {global_bbox[0]:.4f}")
    logger.info(f"  ymin (lat): {global_bbox[1]:.4f}")
    logger.info(f"  xmax (lon): {global_bbox[2]:.4f}")
    logger.info(f"  ymax (lat): {global_bbox[3]:.4f}")

    # Sanity check against India bounds
    india_bounds = (68.0, 6.0, 98.0, 38.0)
    if (
        global_bbox[0] < india_bounds[0]
        or global_bbox[1] < india_bounds[1]
        or global_bbox[2] > india_bounds[2]
        or global_bbox[3] > india_bounds[3]
    ):
        logger.warning("Geometry bounds extend outside expected India extent!")
    else:
        logger.info("Geometry bounds within expected India extent ✓")

    # ── Geometry type distribution ────────────────────────────────────────────
    geom_types = (
        geo_df.with_columns(
            st.geom("geometry").st.geometry_type().alias("geom_type")  # type: ignore[attr-defined]
        )
        .group_by("geom_type")
        .len()
        .sort("len", descending=True)
    )
    logger.info(f"Geometry type distribution:\n{geom_types}")

    # ── bbox column check ─────────────────────────────────────────────────────
    if "bbox" in schema.names():
        bbox_nulls = df.select(pl.col("bbox").is_null().sum()).collect().item()
        if bbox_nulls > 0:
            logger.warning(f"Null bbox values: {bbox_nulls}")
        else:
            logger.info("bbox column present and fully populated ✓")

        # Check bbox struct has expected fields
        bbox_dtype = schema["bbox"]
        logger.info(f"bbox dtype: {bbox_dtype}")
    else:
        logger.warning("No bbox column found — spatial indexing will be slower")

    # ── CRS check via GeoParquet metadata ─────────────────────────────────────
    import json as _json

    import pyarrow.parquet as pq

    first_file = next((f for f in partition_files if f.stat().st_size > 0), None)
    if first_file:
        meta = pq.read_metadata(str(first_file)).metadata or {}
        decoded = {k.decode(): v.decode() for k, v in meta.items()}
        geo_meta = _json.loads(decoded.get("geo", "{}"))

        version = geo_meta.get("version", "NOT FOUND")
        primary_col = geo_meta.get("primary_column", "NOT FOUND")
        geom_col_meta = geo_meta.get("columns", {}).get("geometry", {})
        crs = geom_col_meta.get("crs", "NOT FOUND")
        meta_bbox = geom_col_meta.get("bbox", "NOT FOUND")

        logger.info(f"GeoParquet metadata version: {version}")
        logger.info(f"Primary column: {primary_col}")
        logger.info(f"CRS: {crs}")
        logger.info(f"Metadata bbox: {meta_bbox}")

        if version != "1.1.0":
            logger.warning(f"GeoParquet version is {version}, expected 1.1.0")
        else:
            logger.info("GeoParquet version 1.1.0 ✓")
    else:
        logger.error("No non-empty partition files found for metadata check")

    # ── Sample rows ───────────────────────────────────────────────────────────
    sample = df.limit(3).collect()
    logger.info(f"Sample rows (key columns):\n{sample.select(present_key_cols)}")

    logger.info("=" * 60)
    logger.info("Validity check complete")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "mws.parquet"
    check_geoparquet(path)
