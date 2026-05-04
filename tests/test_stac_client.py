"""Tests for STAC metadata helpers."""

from __future__ import annotations

import pytest

from src.stac.client import (
    StacClientError,
    extract_asset_href,
    extract_proj_code,
    extract_table_columns,
    infer_source_type,
)


def test_extract_table_columns_from_root_or_properties() -> None:
    assert extract_table_columns({"table:columns": [{"name": "uid"}, "state"]}) == {
        "uid",
        "state",
    }
    assert extract_table_columns(
        {"properties": {"table:columns": [{"name": "geometry"}]}}
    ) == {"geometry"}


def test_extract_proj_code_from_root_or_properties() -> None:
    assert extract_proj_code({"proj:code": "EPSG:4326"}) == "EPSG:4326"
    assert extract_proj_code({"properties": {"proj:code": "EPSG:3857"}}) == (
        "EPSG:3857"
    )


def test_extract_asset_href_prefers_data_role() -> None:
    item = {
        "assets": {
            "thumbnail": {"href": "https://example.test/thumb.png"},
            "data": {"href": "s3://bucket/data.parquet", "roles": ["data"]},
        }
    }

    assert extract_asset_href(item) == "s3://bucket/data.parquet"


def test_infer_source_type() -> None:
    assert infer_source_type("s3://bucket/data.parquet") == "s3"
    assert infer_source_type("https://example.test/ows?service=WFS") == "wfs"

    with pytest.raises(StacClientError):
        infer_source_type("https://example.test/file.parquet")
