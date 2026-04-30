"""Benchmark script for MWS GeoParquet output.

Measures:
  - File statistics (sizes, compression ratio, row groups)
  - Cold/warm scan throughput (rows/s)
  - State-filter query latency
  - Null-check query latency
  - Spatial bounds aggregation latency
  - DuckDB analytical query throughput (bbox, group-by)
  - Column projection speed (wide vs narrow reads)

Usage:
    python benchmark.py mws.parquet          # default path
    python benchmark.py /path/to/mws.parquet # custom path
    python benchmark.py mws.parquet --runs 5 # repeat each benchmark N times
"""

import argparse
import json
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import polars as pl
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from loguru import logger

# ── Config ────────────────────────────────────────────────────────────────────

INDIA_BBOX = (68.0, 6.0, 98.0, 38.0)  # xmin, ymin, xmax, ymax


# ── Result container ──────────────────────────────────────────────────────────


@dataclass
class BenchmarkResult:
    """Holds the result of one benchmark case."""

    name: str
    unit: str
    values: list[float] = field(default_factory=list)

    @property
    def mean(self) -> float:
        """Mean of recorded values."""
        return statistics.mean(self.values) if self.values else 0.0

    @property
    def median(self) -> float:
        """Median of recorded values."""
        return statistics.median(self.values) if self.values else 0.0

    @property
    def stdev(self) -> float:
        """Standard deviation of recorded values."""
        return statistics.stdev(self.values) if len(self.values) > 1 else 0.0

    @property
    def min(self) -> float:
        """Minimum value."""
        return min(self.values) if self.values else 0.0

    @property
    def max(self) -> float:
        """Maximum value."""
        return max(self.values) if self.values else 0.0


# ── Timer util ────────────────────────────────────────────────────────────────


class Timer:
    """Context manager that records elapsed seconds."""

    def __init__(self) -> None:
        self.elapsed: float = 0.0

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(self, *_: object) -> None:
        self.elapsed = time.perf_counter() - self._start


# ── Section helpers ───────────────────────────────────────────────────────────


def section(title: str) -> None:
    """Print a section header."""
    width = 62
    print(f"\n{'─' * width}")
    print(f"  {title}")
    print(f"{'─' * width}")


def result_row(
    label: str,
    value: float,
    unit: str,
    note: str = "",
) -> None:
    """Print a formatted result row."""
    note_str = f"  ({note})" if note else ""
    print(f"  {label:<42} {value:>10.2f}  {unit}{note_str}")


def summarize(result: BenchmarkResult) -> None:
    """Print mean/median/stdev for a multi-run result."""
    print(
        f"  {result.name:<42} "
        f"mean={result.mean:.3f}  "
        f"med={result.median:.3f}  "
        f"σ={result.stdev:.3f}  "
        f"[{result.min:.3f}–{result.max:.3f}]  "
        f"{result.unit}"
    )


# ── 1. File statistics ────────────────────────────────────────────────────────


def benchmark_file_stats(output_path: str) -> dict[str, float]:
    """Compute file-level statistics for all partition files.

    Args:
        output_path: Root directory of the partitioned GeoParquet dataset.

    Returns:
        A dictionary with file stat metrics.
    """
    section("1. File Statistics")

    files = sorted(Path(output_path).rglob("*.parquet"))
    if not files:
        print("  No .parquet files found!")
        return {}

    total_bytes = sum(f.stat().st_size for f in files)
    total_mb = total_bytes / (1024**2)
    num_partitions = len(files)

    # Row group stats via pyarrow
    total_rows = 0
    total_rg = 0
    rg_row_counts: list[int] = []
    for f in files:
        meta = pq.read_metadata(str(f))
        total_rows += meta.num_rows
        total_rg += meta.num_row_groups
        for rg in range(meta.num_row_groups):
            rg_row_counts.append(meta.row_group(rg).num_rows)

    avg_file_mb = total_mb / num_partitions
    avg_rg_rows = statistics.mean(rg_row_counts) if rg_row_counts else 0

    result_row("Partition files", num_partitions, "files")
    result_row("Total size on disk", total_mb, "MB")
    result_row("Average partition size", avg_file_mb, "MB/file")
    result_row("Total rows (from metadata)", total_rows, "rows")
    result_row("Total row groups", total_rg, "RGs")
    result_row("Average rows per row group", avg_rg_rows, "rows/RG")
    result_row("Bytes per row", total_bytes / total_rows if total_rows else 0, "B/row")

    return {
        "total_mb": total_mb,
        "num_partitions": num_partitions,
        "total_rows": total_rows,
        "avg_rg_rows": avg_rg_rows,
    }


# ── 2. Cold scan throughput ───────────────────────────────────────────────────


def benchmark_scan_throughput(
    output_path: str,
    total_rows: int,
    runs: int,
) -> None:
    """Measure full-scan throughput with Polars lazy engine.

    Args:
        output_path: Root directory of the dataset.
        total_rows: Total row count for rows/s calculation.
        runs: Number of repeated runs per benchmark.
    """
    section("2. Polars Scan Throughput")

    glob = f"{output_path}/**/*.parquet"

    # Full count scan
    count_result = BenchmarkResult("Full count scan", "s")
    for _ in range(runs):
        with Timer() as t:
            count = (
                pl.scan_parquet(glob, hive_partitioning=True)
                .select(pl.len())
                .collect()
                .item()
            )
        count_result.values.append(t.elapsed)
    rows_per_sec = total_rows / count_result.mean if count_result.mean else 0
    summarize(count_result)
    result_row("  → Rows per second", rows_per_sec, "rows/s", f"count={count:,}")

    # Key-columns projection only
    narrow_result = BenchmarkResult(
        "Key-columns scan (mws_id, version, state, tehsil)", "s"
    )
    for _ in range(runs):
        with Timer() as t:
            pl.scan_parquet(glob, hive_partitioning=True).select(
                ["mws_id", "version", "state", "tehsil"]
            ).collect(engine="streaming")
        narrow_result.values.append(t.elapsed)
    summarize(narrow_result)

    # Full wide scan (all columns)
    wide_result = BenchmarkResult("Full wide scan (all columns)", "s")
    for _ in range(runs):
        with Timer() as t:
            pl.scan_parquet(glob, hive_partitioning=True).collect(engine="streaming")
        wide_result.values.append(t.elapsed)
    summarize(wide_result)


# ── 3. Filter query latency ───────────────────────────────────────────────────


def benchmark_filter_queries(
    output_path: str,
    runs: int,
) -> None:
    """Measure latency for common filter queries.

    Args:
        output_path: Root directory of the dataset.
        runs: Number of repeated runs per benchmark.
    """
    section("3. Filter Query Latency (Polars)")

    glob = f"{output_path}/**/*.parquet"
    df = pl.scan_parquet(glob, hive_partitioning=True)

    states = (
        df.select("state").drop_nulls().unique().limit(3).collect()["state"].to_list()
    )

    for state in states:
        r = BenchmarkResult(f"Filter state='{state}'", "s")
        for _ in range(runs):
            with Timer() as t:
                rows = (
                    df.filter(pl.col("state") == state)
                    .select(pl.len())
                    .collect()
                    .item()
                )
            r.values.append(t.elapsed)
        summarize(r)
        print(f"    → {rows:,} rows returned")

    # Null-state filter
    r_null = BenchmarkResult("Filter state IS NULL", "s")
    for _ in range(runs):
        with Timer() as t:
            null_rows = (
                df.filter(pl.col("state").is_null()).select(pl.len()).collect().item()
            )
        r_null.values.append(t.elapsed)
    summarize(r_null)
    print(f"    → {null_rows:,} rows returned")

    # Null counts across key columns
    key_cols = ["mws_id", "version", "geometry", "state", "district", "tehsil"]
    schema_names = df.collect_schema().names()
    present = [c for c in key_cols if c in schema_names]

    r_nulls = BenchmarkResult("Null-count scan (key columns)", "s")
    for _ in range(runs):
        with Timer() as t:
            df.select([pl.col(c).is_null().sum().alias(c) for c in present]).collect()
        r_nulls.values.append(t.elapsed)
    summarize(r_nulls)


# ── 4. Spatial query latency ──────────────────────────────────────────────────


def benchmark_spatial_queries(
    output_path: str,
    total_rows: int,
    runs: int,
) -> None:
    """Measure bbox aggregation and geometry-type distribution latency.

    Args:
        output_path: Root directory of the dataset.
        total_rows: Total row count for rate calculation.
        runs: Number of repeated runs.
    """
    section("4. Spatial Query Latency (bbox struct column)")

    glob = f"{output_path}/**/*.parquet"
    df = pl.scan_parquet(glob, hive_partitioning=True)
    schema = df.collect_schema()

    if "bbox" not in schema.names():
        print("  bbox column not present — skipping")
        return

    # Global bbox aggregation via bbox struct
    r_bbox = BenchmarkResult("Global bbox min/max via struct", "s")
    for _ in range(runs):
        with Timer() as t:
            res = df.select(
                pl.col("bbox").struct.field("xmin").min().alias("xmin"),
                pl.col("bbox").struct.field("ymin").min().alias("ymin"),
                pl.col("bbox").struct.field("xmax").max().alias("xmax"),
                pl.col("bbox").struct.field("ymax").max().alias("ymax"),
            ).collect()
        r_bbox.values.append(t.elapsed)
    summarize(r_bbox)
    bbox_vals = res.row(0)
    print(f"    → bbox: {[round(v, 4) for v in bbox_vals]}")

    # Validate against India bounds
    xmin, ymin, xmax, ymax = bbox_vals
    ok = (
        xmin >= INDIA_BBOX[0]
        and ymin >= INDIA_BBOX[1]
        and xmax <= INDIA_BBOX[2]
        and ymax <= INDIA_BBOX[3]
    )
    print(f"    → Within India bounds: {'✓' if ok else '✗ WARNING'}")

    # Area stats
    if "area_in_ha" in schema.names():
        r_area = BenchmarkResult("Area stats (min/mean/max)", "s")
        for _ in range(runs):
            with Timer() as t:
                df.select(
                    pl.col("area_in_ha").min().alias("area_min"),
                    pl.col("area_in_ha").mean().alias("area_mean"),
                    pl.col("area_in_ha").max().alias("area_max"),
                ).collect()
            r_area.values.append(t.elapsed)
        summarize(r_area)


# ── 5. DuckDB analytical queries ──────────────────────────────────────────────


def benchmark_duckdb(
    output_path: str,
    total_rows: int,
    runs: int,
) -> None:
    """Measure DuckDB query throughput on the GeoParquet dataset.

    Args:
        output_path: Root directory of the dataset.
        total_rows: Total row count for rate calculation.
        runs: Number of repeated runs.
    """
    section("5. DuckDB Analytical Query Throughput")

    glob = f"{output_path}/**/*.parquet"
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial;")

    queries: list[tuple[str, str]] = [
        (
            "Full count",
            f"SELECT COUNT(*) FROM read_parquet('{glob}', hive_partitioning=true)",
        ),
        (
            "Group-by state count",
            f"""
            SELECT state, COUNT(*) AS n
            FROM read_parquet('{glob}', hive_partitioning=true)
            GROUP BY state ORDER BY n DESC
            """,
        ),
        (
            "Global bbox via bbox struct",
            f"""
            SELECT
                MIN(bbox.xmin) AS xmin,
                MIN(bbox.ymin) AS ymin,
                MAX(bbox.xmax) AS xmax,
                MAX(bbox.ymax) AS ymax
            FROM read_parquet('{glob}', hive_partitioning=true)
            """,
        ),
        (
            "Version distribution",
            f"""
            SELECT version, COUNT(*) AS n
            FROM read_parquet('{glob}', hive_partitioning=true)
            GROUP BY version
            """,
        ),
        (
            "Null state count",
            f"""
            SELECT COUNT(*) FROM read_parquet('{glob}', hive_partitioning=true)
            WHERE state IS NULL
            """,
        ),
    ]

    for label, sql in queries:
        r = BenchmarkResult(label, "s")
        for _ in range(runs):
            with Timer() as t:
                conn.execute(sql).fetchall()
            r.values.append(t.elapsed)
        rows_per_sec = total_rows / r.mean if r.mean else 0
        summarize(r)
        result_row(f"  → {label} throughput", rows_per_sec, "rows/s")

    conn.close()


# ── 6. DuckDB spatial queries ─────────────────────────────────────────────────


def benchmark_spatial_duckdb(
    output_path: str,
    total_rows: int,
    runs: int,
) -> None:
    """Measure DuckDB spatial query performance using the spatial extension.

    Benchmarks:
        - Bounding-box rectangle filter (bbox struct — no geometry decoding)
        - ST_Within(centroid, envelope) — full geometry decode
        - ST_Area computation across all geometries
        - Geometry type distribution via ST_GeometryType
        - Polygon complexity via ST_NPoints

    Args:
        output_path: Root directory of the dataset.
        total_rows: Total row count for throughput calculation.
        runs: Number of repeated runs per query.
    """
    section("6. DuckDB Spatial Queries")

    glob = f"{output_path}/**/*.parquet"
    conn = duckdb.connect()
    conn.execute("INSTALL spatial; LOAD spatial;")

    # Representative test regions (approximate state bounding boxes)
    test_regions = [
        ("Karnataka", 74.0, 11.5, 78.6, 18.5),
        ("Rajasthan", 69.5, 23.0, 78.3, 30.2),
        ("Central India", 76.0, 18.0, 82.0, 24.0),
    ]

    # ── 6a. Bounding-box filter via bbox struct (no geometry decoding) ─────────
    print("\n  [6a] bbox struct range filter (no geometry decoding)")
    for region_name, xmin, ymin, xmax, ymax in test_regions:
        r = BenchmarkResult(f"bbox filter: {region_name}", "s")
        sql = (
            f"SELECT COUNT(*) FROM read_parquet('{glob}', hive_partitioning=true) "
            f"WHERE bbox.xmin >= {xmin} AND bbox.xmax <= {xmax} "
            f"AND bbox.ymin >= {ymin} AND bbox.ymax <= {ymax}"
        )
        count = 0
        for _ in range(runs):
            with Timer() as t:
                row = conn.execute(sql).fetchone()
                count = row[0] if row else 0
            r.values.append(t.elapsed)
        summarize(r)
        print(f"    → {count:,} polygons with bbox inside {region_name}")

    # ── 6b. ST_Within(centroid, envelope) — centroid spatial filter ───────────
    print("\n  [6b] ST_Within(centroid, envelope) — centroid spatial filter")
    for region_name, xmin, ymin, xmax, ymax in test_regions:
        r = BenchmarkResult(f"ST_Within centroid: {region_name}", "s")
        sql = (
            f"SELECT COUNT(*) FROM ("
            f"  SELECT ST_Within("
            f"    ST_Centroid(geometry),"
            f"    ST_MakeEnvelope({xmin}, {ymin}, {xmax}, {ymax})"
            f"  ) AS inside"
            f"  FROM read_parquet('{glob}', hive_partitioning=true)"
            f") WHERE inside = true"
        )
        count = 0
        for _ in range(runs):
            with Timer() as t:
                row = conn.execute(sql).fetchone()
                count = row[0] if row else 0
            r.values.append(t.elapsed)
        rows_per_sec = total_rows / r.mean if r.mean else 0
        summarize(r)
        print(f"    → {count:,} polygon centroids within {region_name}")
        result_row("    → Geometry throughput", rows_per_sec, "rows/s")

    # ── 6c. ST_Area across all polygons ───────────────────────────────────────
    print("\n  [6c] ST_Area (degrees²) across all polygons")
    r_area = BenchmarkResult("ST_Area min/mean/max", "s")
    sql_area = (
        f"SELECT "
        f"MIN(ST_Area(geometry)), "
        f"AVG(ST_Area(geometry)), "
        f"MAX(ST_Area(geometry)) "
        f"FROM read_parquet('{glob}', hive_partitioning=true)"
    )
    area_row: tuple | None = None
    for _ in range(runs):
        with Timer() as t:
            area_row = conn.execute(sql_area).fetchone()
        r_area.values.append(t.elapsed)
    summarize(r_area)
    if area_row:
        print(
            f"    → min={area_row[0]:.6f}  "
            f"mean={area_row[1]:.6f}  "
            f"max={area_row[2]:.6f}  (degrees²)"
        )

    # ── 6d. Geometry type distribution ────────────────────────────────────────
    print("\n  [6d] ST_GeometryType distribution")
    r_type = BenchmarkResult("ST_GeometryType group-by", "s")
    sql_types = (
        f"SELECT ST_GeometryType(geometry) AS gtype, COUNT(*) AS n "
        f"FROM read_parquet('{glob}', hive_partitioning=true) "
        f"GROUP BY gtype ORDER BY n DESC"
    )
    type_rows: list[tuple] = []
    for _ in range(runs):
        with Timer() as t:
            type_rows = conn.execute(sql_types).fetchall()
        r_type.values.append(t.elapsed)
    summarize(r_type)
    for trow in type_rows:
        print(f"    → {trow[0]}: {trow[1]:,}")

    # ── 6e. Polygon complexity via ST_NPoints ──────────────────────────────────
    print("\n  [6e] ST_NPoints — polygon vertex complexity")
    r_npts = BenchmarkResult("ST_NPoints min/mean/max", "s")
    sql_npts = (
        f"SELECT "
        f"MIN(ST_NPoints(geometry)), "
        f"AVG(ST_NPoints(geometry)), "
        f"MAX(ST_NPoints(geometry)) "
        f"FROM read_parquet('{glob}', hive_partitioning=true)"
    )
    npts_row: tuple | None = None
    for _ in range(runs):
        with Timer() as t:
            npts_row = conn.execute(sql_npts).fetchone()
        r_npts.values.append(t.elapsed)
    summarize(r_npts)
    if npts_row:
        print(
            f"    → min={int(npts_row[0])} pts  "
            f"mean={npts_row[1]:.1f} pts  "
            f"max={int(npts_row[2])} pts"
        )

    conn.close()


# ── 7. GeoParquet metadata validation ─────────────────────────────────────────


def benchmark_metadata(output_path: str) -> None:
    """Check GeoParquet metadata correctness and read latency.

    Args:
        output_path: Root directory of the dataset.
    """
    section("7. GeoParquet Metadata")

    files = sorted(Path(output_path).rglob("*.parquet"))
    first = next((f for f in files if f.stat().st_size > 0), None)
    if not first:
        print("  No non-empty files found")
        return

    with Timer() as t:
        raw_meta = pq.read_metadata(str(first)).metadata or {}
    result_row("Metadata read latency", t.elapsed * 1000, "ms")

    decoded = {k.decode(): v.decode() for k, v in raw_meta.items()}
    geo = json.loads(decoded.get("geo", "{}"))

    version = geo.get("version", "NOT FOUND")
    primary = geo.get("primary_column", "NOT FOUND")
    geom_meta = geo.get("columns", {}).get("geometry", {})
    bbox = geom_meta.get("bbox", None)
    covering = geom_meta.get("covering", None)
    crs_auth = (
        geo.get("columns", {})
        .get("geometry", {})
        .get("crs", {})
        .get("id", {})
        .get("authority", "?")
    )
    crs_code = (
        geo.get("columns", {})
        .get("geometry", {})
        .get("crs", {})
        .get("id", {})
        .get("code", "?")
    )

    print(
        f"  GeoParquet spec version : {version}  {'✓' if version == '1.1.0' else '✗'}"
    )
    print(f"  Primary geometry column : {primary}")
    print(f"  CRS                     : {crs_auth}:{crs_code}")
    print(
        f"  Metadata bbox           : {[round(v, 4) for v in bbox] if bbox else 'MISSING ✗'}"
    )
    print(f"  covering declaration    : {'present ✓' if covering else 'MISSING ✗'}")


# ── Main entry point ──────────────────────────────────────────────────────────


def run_benchmarks(output_path: str, runs: int) -> None:
    """Run all benchmarks against a GeoParquet dataset.

    Args:
        output_path: Root directory of the partitioned GeoParquet dataset.
        runs: Number of repeated runs for latency benchmarks.
    """
    print(f"\n{'═' * 62}")
    print("  MWS GeoParquet Benchmark")
    print(f"  Path : {output_path}")
    print(f"  Runs : {runs}")
    print(f"{'═' * 62}")

    stats = benchmark_file_stats(output_path)
    total_rows = int(stats.get("total_rows", 0))

    if total_rows == 0:
        logger.error("No rows found — aborting benchmarks")
        return

    benchmark_scan_throughput(output_path, total_rows, runs)
    benchmark_filter_queries(output_path, runs)
    benchmark_spatial_queries(output_path, total_rows, runs)
    benchmark_duckdb(output_path, total_rows, runs)
    benchmark_spatial_duckdb(output_path, total_rows, runs)
    benchmark_metadata(output_path)

    print(f"\n{'═' * 62}")
    print("  Benchmark complete")
    print(f"{'═' * 62}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Benchmark MWS GeoParquet output performance"
    )
    parser.add_argument(
        "path",
        nargs="?",
        default="mws.parquet",
        help="Path to the partitioned GeoParquet dataset directory",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of runs per benchmark (default: 3)",
    )
    args = parser.parse_args()

    if not Path(args.path).exists():
        print(f"Error: path '{args.path}' does not exist")
        sys.exit(1)

    run_benchmarks(args.path, args.runs)
