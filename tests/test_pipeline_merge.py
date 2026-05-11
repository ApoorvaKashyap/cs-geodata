"""Tests for pipeline merge stage."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from src.pipeline.merge import merge_resolution_frames


def test_merge_static_frames_on_key_drops_duplicate_columns() -> None:
    base = pl.LazyFrame(
        {
            "mws_id": ["m1", "m2"],
            "geometry": ["g1", "g2"],
            "river_basin": ["rb1", "rb2"],
        }
    )
    terrain = pl.LazyFrame(
        {
            "mws_id": ["m1", "m2"],
            "geometry": ["other1", "other2"],
            "terrain_cluster": [1, 2],
        }
    )

    result = merge_resolution_frames([base, terrain], "static", "mws_id").collect()

    assert result.columns == ["mws_id", "geometry", "river_basin", "terrain_cluster"]
    assert result["geometry"].to_list() == ["g1", "g2"]
    assert result["terrain_cluster"].to_list() == [1, 2]


def test_merge_annual_frames_on_key_and_year() -> None:
    ci = pl.LazyFrame({"mws_id": ["m1"], "year": [2021], "ci": [0.8]})
    depth = pl.LazyFrame({"mws_id": ["m1"], "year": [2021], "deltaG": [1.2]})

    result = merge_resolution_frames([ci, depth], "annual", "mws_id").collect()

    assert result.columns == ["mws_id", "year", "ci", "deltaG"]
    assert result["deltaG"].to_list() == [1.2]


def test_merge_fortnightly_frames_on_key_and_date() -> None:
    left = pl.LazyFrame({"mws_id": ["m1"], "date": [date(2021, 1, 1)], "deltaG": [0.5]})
    right = pl.LazyFrame({"mws_id": ["m1"], "date": [date(2021, 1, 1)], "ndvi": [0.7]})

    result = merge_resolution_frames([left, right], "fortnightly", "mws_id").collect()

    assert result.columns == ["mws_id", "date", "deltaG", "ndvi"]
    assert result["ndvi"].to_list() == [0.7]


def test_merge_raises_for_missing_required_key() -> None:
    frame = pl.LazyFrame({"mws_id": ["m1"], "ci": [0.8]})

    with pytest.raises(RuntimeError, match="year"):
        merge_resolution_frames([frame], "annual", "mws_id")
