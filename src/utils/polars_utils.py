"""Polars scan helpers with predicate pushdown for entity materialisation."""

from __future__ import annotations

import polars as pl

_GPU_AVAILABLE: bool | None = None  # None = not yet probed


def _gpu_available() -> bool:
    """Return ``True`` if ``cudf_polars`` is importable (RAPIDS GPU backend).

    The result is cached after the first call so subsequent invocations are
    effectively free.

    Returns:
        bool: True if GPU is available.

    """
    global _GPU_AVAILABLE
    if _GPU_AVAILABLE is None:
        try:
            import cudf_polars  # noqa: F401  # type: ignore[import-untyped]

            _GPU_AVAILABLE = True
            gpu = "cudf_polars (RAPIDS)"
            print("=" * 50)
            print(f"GPU : {gpu} found. Running in GPU mode.")
            print("=" * 50)
        except ModuleNotFoundError:
            _GPU_AVAILABLE = False
            print("=" * 50)
            print("GPU : None found. Running in CPU mode.")
            print("=" * 50)
    return _GPU_AVAILABLE


def collect_lf(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collect a ``LazyFrame`` using the best available backend.

    * **GPU present** — executes via the RAPIDS ``cudf_polars`` streaming
      engine (``GPUEngine(executor="streaming")``).  Handles datasets larger
      than VRAM through data partitioning.
    * **No GPU** — falls back to Polars' built-in CPU streaming executor
      (``collect(streaming=True)``), which keeps memory usage low for large
      Parquet scans.

    Use this function for all *data* scans (materialisation, geometry joins,
    similarity fetches).  Tiny *index* scans that feed into subsequent
    in-process joins should stay as bare ``.collect()`` calls — the streaming
    path can occasionally change row ordering in ways that break those joins.

    Args:
        lf (pl.LazyFrame): The lazy frame to collect.

    Returns:
        pl.DataFrame: A materialised ``pl.DataFrame``.

    """
    global _GPU_AVAILABLE
    if _gpu_available():
        engine = pl.GPUEngine(raise_on_fail=True)
        try:
            result = lf.collect(engine=engine)
            if not isinstance(result, pl.DataFrame):  # pragma: no cover
                raise TypeError(f"Expected pl.DataFrame, got {type(result)}")
            return result
        except Exception as e:
            _GPU_AVAILABLE = False
            print("=" * 50)
            print(
                f"GPU execution failed ({type(e).__name__}: {e}). Falling back to CPU mode."
            )
            print("=" * 50)
    return lf.collect(engine="streaming")


def scan_with_key_filter(
    path: str,
    key_cols: list[str],
    key_values: pl.DataFrame,
    time_expr: pl.Expr | None = None,
) -> pl.LazyFrame:
    """Return a ``pl.LazyFrame`` filtered to the given keys and optional time range.

    Uses ``pl.scan_parquet`` with two predicate-pushdown layers:

    1. **Key filter** — restricts to entity instances whose key column(s) are
       in ``key_values``.  For a single-column key this is an ``is_in``
       predicate pushed down to the Parquet reader.  For composite keys each
       column is filtered independently (over-selects slightly, then pruned
       by the join at collect time).

    2. **Time filter** — an optional Polars expression appended with ``&``,
       also pushed down if the Parquet file carries column statistics.

    Args:
        path (str): Absolute path to a Parquet file.
        key_cols (list[str]): Column name(s) that form the entity's unique key.
        key_values (pl.DataFrame): A narrow ``pl.DataFrame`` containing only the key
            column(s) with the exact values to retain.
        time_expr (pl.Expr | None, optional): An optional Polars filter expression for the time column,
            as produced by :func:`~core_lens.utils.season.resolve_time_filter`.

    Returns:
        pl.LazyFrame: A ``pl.LazyFrame`` ready to be ``.collect()``-ed.

    """
    lf = pl.scan_parquet(path, hive_partitioning=True)

    # Use an inner join to filter the parquet file down to the exact requested keys.
    # This avoids building a massive literal expression tree (which consumes gigabytes
    # of RAM) that occurs when passing large lists to pl.col().is_in().
    if time_expr is not None:
        lf = lf.filter(time_expr)

    lf = lf.join(key_values.lazy(), on=key_cols, how="semi")

    return lf
