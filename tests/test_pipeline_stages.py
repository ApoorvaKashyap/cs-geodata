"""Tests for pipeline normalise and reshape stages."""

from __future__ import annotations

import polars as pl
import pytest

from src.descriptor.schema import LayerDescriptor
from src.pipeline.normalise import normalise
from src.pipeline.reshape import (
    _extract_date,
    _extract_first_year,
    reshape,
)

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _static_layer(**kwargs: object) -> LayerDescriptor:
    defaults: dict[str, object] = {
        "name": "boundaries",
        "resolution": "static",
        "stac_item": "https://example.test/stac/item.json",
    }
    defaults.update(kwargs)
    return LayerDescriptor.model_validate(defaults)


def _annual_layer(**kwargs: object) -> LayerDescriptor:
    defaults: dict[str, object] = {
        "name": "cropping",
        "resolution": "annual",
        "stac_item": "https://example.test/stac/item.json",
        "temporal_pattern": "{col}_{YYYY}_{YYYY}",
    }
    defaults.update(kwargs)
    return LayerDescriptor.model_validate(defaults)


def _fortnightly_layer(**kwargs: object) -> LayerDescriptor:
    defaults: dict[str, object] = {
        "name": "ndvi",
        "resolution": "fortnightly",
        "stac_item": "https://example.test/stac/item.json",
        "temporal_pattern": "{col}_{DATE}",
    }
    defaults.update(kwargs)
    return LayerDescriptor.model_validate(defaults)


# ---------------------------------------------------------------------------
# normalise
# ---------------------------------------------------------------------------


def test_normalise_renames_columns() -> None:
    frame = pl.LazyFrame({"uid": ["a"], "terrainClu": [1]})
    layer = _static_layer(rename={"uid": "mws_id", "terrainClu": "terrain_cluster"})
    result = normalise(frame, layer).collect()
    assert set(result.columns) == {"mws_id", "terrain_cluster"}


def test_normalise_drops_columns() -> None:
    frame = pl.LazyFrame({"uid": ["a"], "area_ha": [100.0], "geometry": ["POINT(0 0)"]})
    layer = _static_layer(drop=["area_ha"])
    result = normalise(frame, layer).collect()
    assert "area_ha" not in result.columns
    assert "uid" in result.columns


def test_normalise_rename_then_drop() -> None:
    frame = pl.LazyFrame({"old_name": [1], "keep_me": [2]})
    layer = _static_layer(rename={"old_name": "new_name"}, drop=["new_name"])
    result = normalise(frame, layer).collect()
    assert "new_name" not in result.columns
    assert "keep_me" in result.columns


def test_normalise_drop_missing_column_is_noop() -> None:
    frame = pl.LazyFrame({"uid": ["a"]})
    layer = _static_layer(drop=["nonexistent"])
    result = normalise(frame, layer).collect()
    assert result.columns == ["uid"]


def test_normalise_no_ops_passthrough() -> None:
    frame = pl.LazyFrame({"uid": ["a"], "val": [1]})
    layer = _static_layer()
    result = normalise(frame, layer).collect()
    assert set(result.columns) == {"uid", "val"}


# ---------------------------------------------------------------------------
# normalise — type coercions
# ---------------------------------------------------------------------------


def test_normalise_coerces_float_string_to_float64() -> None:
    """GeoJSON sources deliver numbers as strings; coercions fix that."""
    frame = pl.LazyFrame({"uid": ["a"], "area": ["1234.5"]})
    layer = _static_layer()
    column_types = {"area": "float64"}
    result = normalise(frame, layer, column_types).collect()
    assert result["area"].dtype == pl.Float64
    assert result["area"][0] == pytest.approx(1234.5)


def test_normalise_coerces_after_rename() -> None:
    """Coercions use the post-rename output column name."""
    frame = pl.LazyFrame({"uid": ["1"], "AREA_HA": ["999.9"]})
    layer = _static_layer(rename={"AREA_HA": "area_ha"})
    column_types = {"AREA_HA": "float64"}
    result = normalise(frame, layer, column_types).collect()
    assert result["area_ha"].dtype == pl.Float64
    assert result["area_ha"][0] == pytest.approx(999.9)


def test_normalise_unknown_stac_type_is_ignored() -> None:
    """Columns with STAC types not in the map are left unchanged."""
    frame = pl.LazyFrame({"uid": ["a"], "geom": ["POINT(0 0)"]})
    layer = _static_layer()
    column_types = {"geom": "geometry"}
    result = normalise(frame, layer, column_types).collect()
    assert result["geom"].dtype == pl.Utf8


def test_normalise_correct_type_is_not_recast() -> None:
    """Columns already at the correct type emit no cast expression."""
    frame = pl.LazyFrame({"uid": ["a"], "val": [1.0]})
    layer = _static_layer()
    column_types = {"val": "float64"}
    result = normalise(frame, layer, column_types).collect()
    assert result["val"].dtype == pl.Float64


def test_normalise_none_column_types_skipped() -> None:
    """None-typed columns in the STAC map are silently skipped."""
    frame = pl.LazyFrame({"uid": ["a"], "val": ["42.0"]})
    layer = _static_layer()
    column_types: dict[str, str | None] = {"val": None}
    result = normalise(frame, layer, column_types).collect()
    assert result["val"].dtype == pl.Utf8


# ---------------------------------------------------------------------------
# reshape — static
# ---------------------------------------------------------------------------


def test_reshape_static_passthrough() -> None:
    frame = pl.LazyFrame({"mws_id": ["x"], "geometry": ["POINT(0 0)"]})
    layer = _static_layer()
    result = reshape(frame, layer, key="mws_id").collect()
    assert set(result.columns) == {"mws_id", "geometry"}
    assert result.height == 1


# ---------------------------------------------------------------------------
# reshape — year extraction
# ---------------------------------------------------------------------------


def test_extract_first_year_double_year() -> None:
    assert _extract_first_year("ci_2017_2018") == 2017


def test_extract_first_year_single_year() -> None:
    assert _extract_first_year("ndvi_2020") == 2020


def test_extract_first_year_raises_if_no_year() -> None:
    with pytest.raises(RuntimeError, match="Cannot extract year"):
        _extract_first_year("no_year_here")


def test_extract_date_standard() -> None:
    assert _extract_date("deltaG_2021-01-15").isoformat() == "2021-01-15"


def test_extract_date_raises_if_no_date() -> None:
    with pytest.raises(RuntimeError, match="Cannot extract date"):
        _extract_date("deltaG_2021_007")


# ---------------------------------------------------------------------------
# reshape — annual melt
# ---------------------------------------------------------------------------


def test_reshape_annual_basic() -> None:
    frame = pl.LazyFrame(
        {
            "mws_id": ["m1", "m2"],
            "ci_2017_2018": [0.8, 0.9],
            "ci_2018_2019": [1.0, 1.1],
        }
    )
    layer = _annual_layer(temporal_pattern="{col}_{YYYY}_{YYYY}")
    result = reshape(frame, layer, key="mws_id").collect()

    assert "mws_id" in result.columns
    assert "year" in result.columns
    assert result.height == 4
    years = sorted(result["year"].to_list())
    assert years == [2017, 2017, 2018, 2018]


def test_reshape_annual_no_temporal_cols_raises() -> None:
    frame = pl.LazyFrame({"mws_id": ["m1"], "static_col": [1]})
    layer = _annual_layer(temporal_pattern="{col}_{YYYY}_{YYYY}")
    with pytest.raises(RuntimeError, match="matched no columns"):
        reshape(frame, layer, key="mws_id")


# ---------------------------------------------------------------------------
# reshape — fortnightly melt
# ---------------------------------------------------------------------------


def test_reshape_fortnightly_basic() -> None:
    frame = pl.LazyFrame(
        {
            "mws_id": ["m1"],
            "deltaG_2021-01-01": [0.5],
            "deltaG_2021-01-15": [0.6],
        }
    )
    layer = _fortnightly_layer(temporal_pattern="{col}_{DATE}")
    result = reshape(frame, layer, key="mws_id").collect()

    assert "mws_id" in result.columns
    assert "date" in result.columns
    assert "year" not in result.columns
    assert "fortnight" not in result.columns
    assert result.height == 2
    assert sorted(date.isoformat() for date in result["date"].to_list()) == [
        "2021-01-01",
        "2021-01-15",
    ]
