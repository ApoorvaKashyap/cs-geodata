"""Tests for data acquisition sources."""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import httpx
import polars as pl
import pytest

from src.sources.s3 import S3Source, _geojson_to_lazyframe, _read_file
from src.sources.wfs import (
    TehsilKey,
    _child_links,
    _derive_catalogue_root,
    walk_stac_tehsil_hierarchy,
)

# ---------------------------------------------------------------------------
# S3Source helpers
# ---------------------------------------------------------------------------


def test_geojson_to_lazyframe_basic() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "test.geojson"
        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"id": "1", "name": "alpha"},
                    "geometry": {"type": "Point", "coordinates": [78.0, 20.0]},
                },
                {
                    "type": "Feature",
                    "properties": {"id": "2", "name": "beta"},
                    "geometry": None,
                },
            ],
        }
        path.write_text(json.dumps(geojson), encoding="utf-8")
        frame = _geojson_to_lazyframe(path).collect()

    assert set(frame.columns) >= {"id", "name", "geometry"}
    assert frame.height == 2
    assert frame["id"].to_list() == ["1", "2"]
    assert frame["geometry"][1] is None


def test_geojson_to_lazyframe_empty_raises() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        path = Path(tmpdir) / "empty.geojson"
        path.write_text(json.dumps({"type": "FeatureCollection", "features": []}))
        with pytest.raises(RuntimeError, match="no features"):
            _geojson_to_lazyframe(path)


def test_read_file_parquet(tmp_path: Path) -> None:
    parquet_path = tmp_path / "data.parquet"
    pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}).write_parquet(parquet_path)
    frame = _read_file(parquet_path).collect()
    assert frame.height == 2
    assert set(frame.columns) == {"a", "b"}


def test_read_file_unsupported_raises(tmp_path: Path) -> None:
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("a,b\n1,x\n")
    with pytest.raises(RuntimeError, match="Unsupported source file type"):
        _read_file(csv_path)


def test_s3_source_fetch_calls_download_then_reads(tmp_path: Path) -> None:
    parquet_path = tmp_path / "source.parquet"
    pl.DataFrame({"uid": ["A"], "value": [1.0]}).write_parquet(parquet_path)

    source = S3Source(asset_href="s3://bucket/key.parquet", scratch_dir=tmp_path)

    with patch.object(source, "_download", return_value=parquet_path):
        frame = asyncio.run(source.fetch()).collect()

    assert frame.height == 1
    assert "uid" in frame.columns


# ---------------------------------------------------------------------------
# WFS STAC catalogue walker helpers
# ---------------------------------------------------------------------------


def test_tehsil_key_equality() -> None:
    a = TehsilKey(state="Karnataka", district="Dharwad", tehsil="Hubli")
    b = TehsilKey(state="Karnataka", district="Dharwad", tehsil="Hubli")
    assert a == b


def test_tehsil_key_inequality() -> None:
    a = TehsilKey(state="Karnataka", district="Dharwad", tehsil="Hubli")
    b = TehsilKey(state="Karnataka", district="Dharwad", tehsil="Gadag")
    assert a != b


def test_child_links_extracts_names() -> None:
    doc = {
        "links": [
            {"rel": "child", "href": "https://stac.example.com/root/karnataka/"},
            {"rel": "child", "href": "https://stac.example.com/root/maharashtra/"},
            {"rel": "self", "href": "https://stac.example.com/root/"},
        ]
    }
    names = _child_links(doc)
    assert names == ["karnataka", "maharashtra"]


def test_child_links_ignores_non_child_rels() -> None:
    doc = {
        "links": [
            {"rel": "self", "href": "https://stac.example.com/root/"},
            {"rel": "root", "href": "https://stac.example.com/"},
        ]
    }
    assert _child_links(doc) == []


def test_child_links_empty_for_non_dict() -> None:
    assert _child_links("not a dict") == []


def test_child_links_item_rel_included() -> None:
    doc = {
        "links": [
            {"rel": "item", "href": "https://stac.example.com/root/hubli/item.json"},
        ]
    }
    names = _child_links(doc)
    assert names == ["item.json"]


def test_derive_catalogue_root_with_marker() -> None:
    uri = "https://stac.example.com/tehsil_wise/karnataka/dharwad/hubli/item.json"
    root = _derive_catalogue_root(uri)
    assert root == "https://stac.example.com/tehsil_wise/"


def test_derive_catalogue_root_fallback() -> None:
    uri = "https://stac.example.com/collections/mws/items/item_1"
    root = _derive_catalogue_root(uri)
    assert root.endswith("/")


def test_walk_stac_tehsil_hierarchy_throttles_catalogue_fetches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    active = 0
    max_active = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1

        url = str(request.url)
        if url == "https://stac.example.com/tehsil_wise/":
            return _stac_response(["state_a", "state_b", "state_c"])
        if (
            url.endswith("/state_a/")
            or url.endswith("/state_b/")
            or url.endswith("/state_c/")
        ):
            return _stac_response(["district_1", "district_2"])
        return _stac_response(["tehsil_1", "tehsil_2"])

    transport = httpx.MockTransport(handler)
    original_async_client = httpx.AsyncClient

    def async_client_factory(*args: object, **kwargs: object) -> httpx.AsyncClient:
        kwargs["transport"] = transport
        return original_async_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", async_client_factory)

    keys = asyncio.run(
        walk_stac_tehsil_hierarchy(
            "https://stac.example.com/tehsil_wise/state_a/district_1/tehsil_1/item.json",
            max_concurrent_fetches=2,
        )
    )

    assert len(keys) == 12
    assert max_active <= 2


# ---------------------------------------------------------------------------
# Per-tehsil fetch job
# ---------------------------------------------------------------------------


def _make_httpx_handler(status_code: int, json_body: object = None):
    """Return a callable that httpx uses as a transport handler."""
    import httpx

    def handler(request: httpx.Request) -> httpx.Response:
        if json_body is not None:
            return httpx.Response(status_code, json=json_body)
        return httpx.Response(status_code)

    return handler


def _stac_response(children: list[str]) -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "links": [
                {"rel": "child", "href": f"https://stac.example.com/{child}/"}
                for child in children
            ]
        },
    )


def test_fetch_tehsil_job_writes_parquet(tmp_path: Path) -> None:
    """fetch_tehsil_job writes a valid Parquet file from a GeoJSON response."""
    from unittest.mock import patch

    import httpx

    from src.pipeline.fetch_job import fetch_tehsil_job

    geojson_response = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"uid": "T1_001", "area": "1234.5"},
                "geometry": {"type": "Point", "coordinates": [77.0, 15.0]},
            }
        ],
    }

    output_path = tmp_path / "T1.parquet"
    dummy_request = httpx.Request("GET", "https://geoserver.example.com/wfs")
    mock_response = httpx.Response(200, json=geojson_response, request=dummy_request)

    with patch("httpx.get", return_value=mock_response):
        fetch_tehsil_job("https://geoserver.example.com/wfs", str(output_path))

    assert output_path.exists()
    df = pl.read_parquet(output_path)
    assert df.height == 1
    assert "uid" in df.columns
    assert "geometry" in df.columns


def test_fetch_tehsil_job_empty_response_writes_empty_parquet(tmp_path: Path) -> None:
    """fetch_tehsil_job writes an empty Parquet file when features list is empty."""
    from unittest.mock import patch

    import httpx

    from src.pipeline.fetch_job import fetch_tehsil_job

    output_path = tmp_path / "empty.parquet"
    dummy_request = httpx.Request("GET", "https://geoserver.example.com/wfs/empty")
    mock_response = httpx.Response(
        200, json={"type": "FeatureCollection", "features": []}, request=dummy_request
    )

    with patch("httpx.get", return_value=mock_response):
        fetch_tehsil_job("https://geoserver.example.com/wfs/empty", str(output_path))

    assert output_path.exists()


def test_fetch_tehsil_job_http_error_raises(tmp_path: Path) -> None:
    """fetch_tehsil_job raises RuntimeError on HTTP 4xx/5xx responses."""
    from unittest.mock import patch

    import httpx

    from src.pipeline.fetch_job import fetch_tehsil_job

    output_path = tmp_path / "fail.parquet"

    def raise_http_error(*args: object, **kwargs: object) -> None:
        raise httpx.HTTPStatusError(
            "503 Service Unavailable",
            request=httpx.Request("GET", "https://geoserver.example.com/wfs/bad"),
            response=httpx.Response(503),
        )

    with patch("httpx.get", side_effect=raise_http_error):
        with pytest.raises(RuntimeError, match="WFS fetch failed"):
            fetch_tehsil_job("https://geoserver.example.com/wfs/bad", str(output_path))
