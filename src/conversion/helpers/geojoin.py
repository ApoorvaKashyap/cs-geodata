import polars as pl
from loguru import logger

from src.conversion.helpers.duckdb_funcs import init_duckdb

BATCH_SIZE = 25_000


def fill_missing_admin_boundaries(
    merged: pl.LazyFrame,
    tehsils_path: str,
) -> pl.LazyFrame:
    logger.info("Loading tehsil boundaries")

    has_admin = merged.filter(pl.col("state").is_not_null())
    needs_admin = merged.filter(pl.col("state").is_null())

    # Only collect the columns needed for the spatial join (lightweight)
    join_keys = needs_admin.select(["mws_id", "version", "geometry"]).collect(
        engine="streaming"
    )

    row_count = join_keys.height
    logger.info(f"Filling admin boundaries for {row_count} MWS polygons")

    if row_count == 0:
        logger.info("No missing admin boundaries — skipping spatial join")
        return merged

    conn = init_duckdb()
    try:
        # Load tehsil boundaries once into a persistent DuckDB table
        conn.execute(f"""
            CREATE TABLE tehsils AS
            SELECT
                STATE AS state,
                District AS district,
                TEHSIL AS tehsil,
                geom AS geometry
            FROM ST_Read('{tehsils_path}')
        """)
        logger.info("Tehsil boundaries loaded into DuckDB")

        # Process in batches to avoid OOM — only 3 columns per batch
        lookup_frames: list[pl.DataFrame] = []
        total_matched = 0
        total_unmatched = 0

        for batch_start in range(0, row_count, BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, row_count)
            batch = join_keys.slice(batch_start, batch_end - batch_start)
            logger.info(
                f"Processing batch {batch_start}-{batch_end} ({batch.height} rows)"
            )

            conn.register("batch_table", batch.to_arrow())

            sql = """
                SELECT
                    b.mws_id,
                    b.version,
                    t.state,
                    t.district,
                    t.tehsil
                FROM (
                    SELECT
                        mws_id,
                        version,
                        ST_Centroid(ST_GeomFromWKB(geometry)) AS centroid
                    FROM batch_table
                ) b
                LEFT JOIN tehsils t
                    ON ST_Within(b.centroid, t.geometry)
            """
            result = conn.execute(sql).fetch_arrow_table()
            batch_lookup = pl.DataFrame(pl.from_arrow(result)).with_columns(
                # Normalize: strip "(Disputed)" suffix and "Disputed " prefix,
                # e.g. "MADHYA PRADESH(DISPUTED)" → "Madhya Pradesh"
                #      "DISPUTED (WEST BENGAL)"   → "West Bengal"
                pl.col("state")
                .str.replace(r"(?i)\s*\(disputed\)\s*", "")
                .str.replace(r"(?i)^disputed\s*", "")
                .str.replace_all(r"[()]", "")
                .str.strip_chars()
                .str.to_titlecase(),
                pl.col("district").str.to_titlecase(),
                pl.col("tehsil").str.to_titlecase(),
            )

            matched = batch_lookup.filter(pl.col("state").is_not_null()).height
            unmatched = batch_lookup.filter(pl.col("state").is_null()).height
            total_matched += matched
            total_unmatched += unmatched
            logger.info(f"  Batch result: {matched} matched, {unmatched} unmatched")

            lookup_frames.append(batch_lookup)
            conn.unregister("batch_table")

        # Concat the lightweight lookup (only 5 columns: mws_id, version,
        # state, district, tehsil) — ~253K × 5 instead of ~253K × 2584
        admin_lookup = pl.concat(lookup_frames, how="diagonal_relaxed")

        # Deduplicate: centroids on tehsil boundaries can match
        # multiple tehsils via ST_Within, producing 2 rows per polygon
        pre_dedup = admin_lookup.height
        admin_lookup = admin_lookup.unique(subset=["mws_id", "version"])
        deduped = pre_dedup - admin_lookup.height
        if deduped > 0:
            logger.info(f"Removed {deduped} boundary-overlap duplicates")

        logger.info(
            f"Admin boundary fill: {total_matched} matched, {total_unmatched} unmatched"
        )
    finally:
        conn.close()

    logger.info("Admin boundary fill complete")

    # Join the admin lookup back onto the full needs_admin lazily
    # (avoids materializing 2584 columns during spatial join)
    filled = needs_admin.drop(["state", "district", "tehsil"]).join(
        admin_lookup.lazy(),
        on=["mws_id", "version"],
        how="left",
    )

    return pl.concat(
        [has_admin, filled],
        how="diagonal_relaxed",
    )
