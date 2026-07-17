import polars as pl
from loguru import logger

from src.conversion.helpers.duckdb_funcs import init_duckdb

BATCH_SIZE = 25_000

# Regex patterns used to normalise disputed-territory state name strings.
_DISPUTED_SUFFIX_RE = r"(?i)\s*\(disputed\)\s*"
_DISPUTED_PREFIX_RE = r"(?i)^disputed\s*"
_PARENS_RE = r"[()]"


def _normalise_admin_col(col: pl.Expr) -> pl.Expr:
    """Strip disputed-territory suffixes/prefixes and apply title-case to an admin name column."""
    return (
        col.str.replace(_DISPUTED_SUFFIX_RE, "")
        .str.replace(_DISPUTED_PREFIX_RE, "")
        .str.replace_all(_PARENS_RE, "")
        .str.strip_chars()
        .str.to_titlecase()
    )


def fill_missing_admin_boundaries(
    merged: pl.LazyFrame,
    tehsils_path: str,
    entity_key: str = "mws_id",
) -> pl.LazyFrame:
    """Fill missing administrative boundaries using a polygon-intersection spatial join.

    Each entity polygon is intersected against the tehsil boundaries and assigned
    **all** overlapping tehsils as a sorted, deduplicated list.  This correctly
    handles entities (e.g. large watersheds) that span more than one tehsil.

    Entities that already carry admin data have their scalar ``state``,
    ``district``, and ``tehsil`` columns wrapped into 1-element lists so that
    the final output schema is uniform: every row has ``List[String]`` for all
    three admin columns.

    Batches the intersection join to avoid memory limits in DuckDB.

    Args:
        merged: LazyFrame containing the merged entity data.
        tehsils_path: Path to the raw tehsil boundaries shapefile/geopackage.
        entity_key: Primary-key column name. Defaults to ``'mws_id'``.

    Returns:
        A LazyFrame where ``state``, ``district``, and ``tehsil`` are typed as
        ``List[String]``.  Entities with no intersecting tehsil carry empty
        lists for those fields.
    """
    logger.info("Loading tehsil boundaries")

    # Treat both nulls and empty/whitespace strings as missing admin data.
    is_missing = pl.col("state").is_null() | (
        pl.col("state").cast(pl.String).str.strip_chars() == ""
    )

    # Wrap existing scalar admin columns into 1-element lists so the final
    # concat produces a uniform List[String] schema across all rows.
    has_admin = merged.filter(~is_missing).with_columns(
        pl.col("state").map_elements(
            lambda x: [x] if x else [], return_dtype=pl.List(pl.String)
        ),
        pl.col("district").map_elements(
            lambda x: [x] if x else [], return_dtype=pl.List(pl.String)
        ),
        pl.col("tehsil").map_elements(
            lambda x: [x] if x else [], return_dtype=pl.List(pl.String)
        ),
    )
    needs_admin = merged.filter(is_missing)

    # Only collect the columns needed for the spatial join (lightweight).
    join_keys = needs_admin.select([entity_key, "geometry"]).collect(engine="streaming")

    row_count = join_keys.height
    logger.info(
        f"Filling admin boundaries for {row_count} entity polygons via intersection"
    )

    if row_count == 0:
        logger.info("No missing admin boundaries — skipping spatial join")
        return merged

    conn = init_duckdb()
    try:
        # Load tehsil boundaries once into a persistent DuckDB table.
        sql_tehsils = f"""
            CREATE TABLE tehsils AS
            SELECT
                STATE    AS state,
                District AS district,
                TEHSIL   AS tehsil,
                geom     AS geometry
            FROM ST_Read('{tehsils_path}')
        """
        conn.execute(sql_tehsils)
        logger.info("Tehsil boundaries loaded into DuckDB")

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

            # Polygon-intersection join: returns one row per (entity, tehsil) pair.
            # Polars aggregates into sorted unique lists after normalisation.
            sql = f"""
                SELECT
                    b.{entity_key},
                    t.state,
                    t.district,
                    t.tehsil
                FROM (
                    SELECT
                        {entity_key},
                        ST_GeomFromWKB(geometry) AS entity_geom
                    FROM batch_table
                ) b
                LEFT JOIN tehsils t
                    ON ST_Intersects(b.entity_geom, t.geometry)
            """
            result = conn.execute(sql).fetch_arrow_table()

            # Normalise admin names (title-case, strip disputed-territory labels).
            batch_df = pl.DataFrame(pl.from_arrow(result)).with_columns(
                _normalise_admin_col(pl.col("state")),
                pl.col("district").str.to_titlecase(),
                pl.col("tehsil").str.to_titlecase(),
            )

            # Aggregate: one row per entity, each admin column becomes a sorted
            # unique list of all intersecting values.
            batch_lookup = batch_df.group_by(entity_key).agg(
                pl.col("state").drop_nulls().unique().sort(),
                pl.col("district").drop_nulls().unique().sort(),
                pl.col("tehsil").drop_nulls().unique().sort(),
            )

            matched = batch_lookup.filter(pl.col("state").list.len() > 0).height
            unmatched = batch_lookup.filter(pl.col("state").list.len() == 0).height
            total_matched += matched
            total_unmatched += unmatched
            logger.info(f"  Batch result: {matched} matched, {unmatched} unmatched")

            lookup_frames.append(batch_lookup)
            conn.unregister("batch_table")

        admin_lookup = pl.concat(lookup_frames, how="diagonal_relaxed")

        logger.info(
            f"Admin boundary fill: {total_matched} matched, {total_unmatched} unmatched"
        )
    finally:
        conn.close()

    logger.info("Admin boundary fill complete")

    # Join the admin lookup (3 list columns + entity_key) back onto needs_admin
    # lazily — avoids materialising all columns during the spatial join.
    filled = needs_admin.drop(["state", "district", "tehsil"]).join(
        admin_lookup.lazy(),
        on=entity_key,
        how="left",
    )

    return pl.concat(
        [has_admin, filled],
        how="diagonal_relaxed",
    )
