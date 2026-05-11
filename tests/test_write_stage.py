"""Tests for pipeline write stage helpers."""

from __future__ import annotations

from pathlib import Path

import fsspec
import polars as pl
import pyarrow as pa
import shapely.wkb
from shapely.geometry import Point

from src.descriptor.schema import EntityDescriptor
from src.pipeline.write import (
    _compute_bbox,
    _is_s3_uri,
    _join_uri,
    _upload_entity_output,
    write_resolution,
)
from src.stac.client import StacSourceInfo


def _descriptor() -> EntityDescriptor:
    return EntityDescriptor.model_validate(
        {
            "entity": "mws",
            "key": "mws_id",
            "geometry": "geometry",
            "partition_by": "river_basin",
            "layers": [
                {
                    "name": "annual",
                    "resolution": "annual",
                    "stac_item": "stac",
                    "temporal_pattern": "{col}_{YYYY}",
                }
            ],
        }
    )


def _source_info() -> StacSourceInfo:
    return StacSourceInfo(
        stac_item_uri="stac",
        columns=set(),
        column_types={},
        crs="EPSG:4326",
        asset_href="s3://bucket/source.parquet",
        source_type="s3",
    )


def test_s3_uri_helpers() -> None:
    assert _is_s3_uri("s3://bucket/out")
    assert not _is_s3_uri("/tmp/out")
    assert _join_uri("s3://bucket/out/", "/mws/", "annual.parquet") == (
        "s3://bucket/out/mws/annual.parquet"
    )


def test_upload_entity_output_copies_nested_files(
    tmp_path: Path,
    monkeypatch,
) -> None:
    local_entity_dir = tmp_path / "mws"
    nested_dir = local_entity_dir / "static.geoparquet" / "river_basin=rb1"
    nested_dir.mkdir(parents=True)
    (nested_dir / "part-0.parquet").write_bytes(b"data")
    (local_entity_dir / "annual.parquet").write_bytes(b"annual")

    monkeypatch.setattr("src.pipeline.write.get_s3_storage_options", lambda: {})

    fs = fsspec.filesystem("memory")
    fs.store.clear()

    _upload_entity_output(local_entity_dir, "memory://bucket/ecolib/mws")

    assert fs.exists("/bucket/ecolib/mws/annual.parquet")
    assert fs.exists(
        "/bucket/ecolib/mws/static.geoparquet/river_basin=rb1/part-0.parquet"
    )


def test_write_resolution_s3_writes_locally_then_uploads(monkeypatch) -> None:
    uploads: list[tuple[Path, str]] = []

    def fake_write_local(
        frame,
        resolution,
        descriptor,
        source_info,
        output_root,
    ) -> None:
        entity_dir = output_root / descriptor.entity
        entity_dir.mkdir(parents=True)
        (entity_dir / f"{resolution}.parquet").write_bytes(b"data")

    def fake_upload(local_entity_dir: Path, remote_entity_uri: str) -> None:
        uploads.append((local_entity_dir, remote_entity_uri))
        assert (local_entity_dir / "annual.parquet").exists()

    monkeypatch.setattr("src.pipeline.write._write_resolution_local", fake_write_local)
    monkeypatch.setattr("src.pipeline.write._upload_entity_output", fake_upload)

    write_resolution(
        pl.LazyFrame({"mws_id": ["m1"], "year": [2021], "ci": [0.8]}),
        "annual",
        _descriptor(),
        _source_info(),
        "s3://bucket/ecolib",
    )

    assert len(uploads) == 1
    assert uploads[0][1] == "s3://bucket/ecolib/mws"


def test_compute_bbox_from_wkb_geometry() -> None:
    table = pa.table(
        {
            "geometry": [
                shapely.wkb.dumps(Point(1, 2)),
                shapely.wkb.dumps(Point(3, 4)),
                None,
            ]
        }
    )

    assert _compute_bbox(table, "geometry") == (1.0, 2.0, 3.0, 4.0)


def test_compute_bbox_ignores_invalid_wkb() -> None:
    table = pa.table({"geometry": [b"not-wkb"]})

    assert _compute_bbox(table, "geometry") is None
