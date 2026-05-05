"""Tests for STAC metadata helpers."""

from __future__ import annotations

import pytest

from src.stac.client import (
    StacClient,
    StacClientError,
    extract_asset_href,
    extract_proj_code,
    extract_table_column_types,
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


def test_extract_table_column_types() -> None:
    item = {
        "table:columns": [
            {"name": "uid", "type": "string"},
            {"name": "area", "type": "float"},
            {"name": "geometry"},
            "state",
        ]
    }

    assert extract_table_column_types(item) == {
        "uid": "string",
        "area": "float",
        "geometry": None,
    }


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


def test_resolve_source_info() -> None:
    item = {
        "properties": {
            "table:columns": [
                {"name": "uid", "type": "string"},
                {"name": "STATE", "type": "string"},
            ],
            "proj:code": "EPSG:4326",
        },
        "assets": {"data": {"href": "s3://bucket/tehsil.parquet", "roles": ["data"]}},
    }

    source_info = StacClient(timeout_seconds=1).resolve_source_info(
        "https://example.test/stac/tehsil.json",
        item,
    )

    assert source_info.columns == {"uid", "STATE"}
    assert source_info.column_types == {"uid": "string", "STATE": "string"}
    assert source_info.crs == "EPSG:4326"
    assert source_info.asset_href == "s3://bucket/tehsil.parquet"
    assert source_info.source_type == "s3"
