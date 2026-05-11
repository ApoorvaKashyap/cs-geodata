"""Pipeline reshape stage — wide-to-long melt for temporal layers."""

from __future__ import annotations

import re
from datetime import date

import polars as pl

from src.descriptor.schema import LayerDescriptor
from src.descriptor.validator import (
    temporal_pattern_to_col_regex,
    temporal_pattern_to_regex,
)


def reshape(frame: pl.LazyFrame, layer: LayerDescriptor, key: str) -> pl.LazyFrame:
    """Reshape a normalised LazyFrame for the declared layer resolution.

    Static layers pass through unchanged. Annual and fortnightly layers are
    melted from wide temporal columns to long format.

    Output schemas:
        - annual: ``(key, year, col1, col2, ...)``
        - fortnightly: ``(key, date, col1, col2, ...)``

    For temporal column names that encode two years (e.g. ``ci_2017_2018``),
    only the first year is kept (2017).

    Args:
        frame: Normalised LazyFrame.
        layer: Layer Descriptor declaring resolution and temporal pattern.
        key: Primary key column name (already renamed).

    Returns:
        Reshaped LazyFrame.

    Raises:
        RuntimeError: If a temporal layer has no matchable temporal columns.
    """
    if layer.resolution == "static":
        return frame

    if layer.temporal_pattern is None:
        raise RuntimeError(
            f"Layer '{layer.name}' is temporal but has no temporal_pattern."
        )

    pattern = temporal_pattern_to_regex(layer.temporal_pattern)
    col_pattern = temporal_pattern_to_col_regex(layer.temporal_pattern)
    columns = frame.collect_schema().names()
    temporal_cols = [col for col in columns if pattern.fullmatch(col)]

    if not temporal_cols:
        raise RuntimeError(
            f"Layer '{layer.name}': temporal_pattern '{layer.temporal_pattern}' "
            f"matched no columns in the frame."
        )

    id_cols = [col for col in columns if col not in set(temporal_cols)]
    value_col_names = _extract_value_col_names(
        temporal_cols, col_pattern, layer.temporal_pattern
    )

    if layer.resolution == "annual":
        return _melt_annual(frame, id_cols, temporal_cols, value_col_names, key)

    return _melt_fortnightly(frame, id_cols, temporal_cols, value_col_names, key)


def _melt_annual(
    frame: pl.LazyFrame,
    id_cols: list[str],
    temporal_cols: list[str],
    value_col_names: dict[str, str],
    key: str,
) -> pl.LazyFrame:
    melted = frame.unpivot(
        on=temporal_cols,
        index=id_cols,
        variable_name="_temporal_col",
        value_name="_value",
    )

    year_map = (
        pl.Series(
            "_temporal_col",
            temporal_cols,
        )
        .to_frame()
        .with_columns(
            pl.Series("year", [_extract_first_year(col) for col in temporal_cols]),
            pl.Series("_base_col", [value_col_names[col] for col in temporal_cols]),
        )
    )

    melted = melted.join(year_map.lazy(), on="_temporal_col", how="left")
    return _pivot_base_cols(melted, id_cols, key)


def _melt_fortnightly(
    frame: pl.LazyFrame,
    id_cols: list[str],
    temporal_cols: list[str],
    value_col_names: dict[str, str],
    key: str,
) -> pl.LazyFrame:
    melted = frame.unpivot(
        on=temporal_cols,
        index=id_cols,
        variable_name="_temporal_col",
        value_name="_value",
    )

    mapping = pl.DataFrame(
        {
            "_temporal_col": temporal_cols,
            "date": [_extract_date(col) for col in temporal_cols],
            "_base_col": [value_col_names[col] for col in temporal_cols],
        }
    )

    melted = melted.join(mapping.lazy(), on="_temporal_col", how="left")
    return _pivot_base_cols_fortnightly(melted, id_cols, key)


def _pivot_base_cols(
    melted: pl.LazyFrame,
    id_cols: list[str],
    key: str,
) -> pl.LazyFrame:
    collected = melted.collect()
    base_cols = collected["_base_col"].unique().to_list()

    pivoted = (
        collected.pivot(
            on="_base_col",
            index=[*id_cols, "year"],
            values="_value",
            aggregate_function="first",
        )
        .lazy()
        .drop("_temporal_col", strict=False)
    )

    time_cols = ["year"]
    non_key_id = [c for c in id_cols if c != key]
    final_cols = [key, *time_cols, *non_key_id, *base_cols]
    available = [c for c in final_cols if c in set(pivoted.collect_schema().names())]
    return pivoted.select(available)


def _pivot_base_cols_fortnightly(
    melted: pl.LazyFrame,
    id_cols: list[str],
    key: str,
) -> pl.LazyFrame:
    collected = melted.collect()
    base_cols = collected["_base_col"].unique().to_list()

    pivoted = (
        collected.pivot(
            on="_base_col",
            index=[*id_cols, "date"],
            values="_value",
            aggregate_function="first",
        )
        .lazy()
        .drop("_temporal_col", strict=False)
    )

    time_cols = ["date"]
    non_key_id = [c for c in id_cols if c != key]
    final_cols = [key, *time_cols, *non_key_id, *base_cols]
    available = [c for c in final_cols if c in set(pivoted.collect_schema().names())]
    return pivoted.select(available)


def _extract_value_col_names(
    temporal_cols: list[str],
    col_pattern: re.Pattern[str],
    temporal_pattern: str,
) -> dict[str, str]:
    has_col_token = "{col}" in temporal_pattern
    result: dict[str, str] = {}
    for col in temporal_cols:
        if has_col_token:
            match = col_pattern.fullmatch(col)
            if match and match.lastindex and match.lastindex >= 1:
                result[col] = match.group(1)
            else:
                result[col] = col
        else:
            result[col] = col
    return result


def _extract_first_year(col_name: str) -> int:
    years = re.findall(r"\d{4}", col_name)
    if not years:
        raise RuntimeError(f"Cannot extract year from column name '{col_name}'.")
    return int(years[0])


def _extract_date(col_name: str) -> date:
    date_match = re.search(r"\d{4}-\d{2}-\d{2}", col_name)
    if not date_match:
        raise RuntimeError(f"Cannot extract date from column name '{col_name}'.")
    return date.fromisoformat(date_match.group())
