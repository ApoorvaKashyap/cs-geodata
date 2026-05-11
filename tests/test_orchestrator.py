"""Tests for pipeline orchestrator jobs."""

from __future__ import annotations

import asyncio

import polars as pl
import pytest

from src.descriptor.schema import EntityDescriptor
from src.pipeline.orchestrator import _run_pipeline_stages, run_descriptor_job
from src.stac.client import StacSourceInfo

DESCRIPTOR_DATA = {
    "entity": "tehsil",
    "key": "tehsil_id",
    "geometry": "geometry",
    "partition_by": "state",
    "layers": [
        {
            "name": "boundaries",
            "resolution": "static",
            "stac_item": "https://example.test/stac/tehsil.json",
            "rename": {},
            "drop": [],
            "temporal_pattern": None,
        }
    ],
}

STAC_SOURCE_INFOS = {
    "https://example.test/stac/tehsil.json": {
        "columns": ["uid", "state", "geometry"],
        "column_types": {"uid": "str", "state": "str", "geometry": "str"},
        "crs": "EPSG:4326",
        "asset_href": "s3://bucket/tehsil.parquet",
        "source_type": "s3",
    }
}


def test_run_descriptor_job_marks_running_and_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, str | None]] = []

    class FakeProgressStore:
        """Progress store double for successful jobs."""

        def mark_descriptor_running(
            self,
            run_id: str,
            entity: str,
            phase: str,
            layer: str | None = None,
        ) -> None:
            """Record running progress."""
            calls.append(("running", run_id, entity))

        def mark_descriptor_complete(self, run_id: str, entity: str) -> None:
            """Record complete progress."""
            calls.append(("complete", run_id, entity))

        def mark_descriptor_failed(
            self,
            run_id: str,
            entity: str,
            error: str,
        ) -> None:
            """Record failed progress."""
            calls.append(("failed", run_id, entity))

        def set_descriptor_status(self, *args: object, **kwargs: object) -> None:
            """No-op for progress updates."""

    monkeypatch.setattr("src.pipeline.orchestrator.ProgressStore", FakeProgressStore)
    monkeypatch.setattr(
        "src.pipeline.orchestrator._run_pipeline_stages",
        lambda *args, **kwargs: None,
    )

    async def fake_stages(*args: object, **kwargs: object) -> None:
        pass

    monkeypatch.setattr("src.pipeline.orchestrator._run_pipeline_stages", fake_stages)

    run_descriptor_job(
        "run-1", DESCRIPTOR_DATA, "s3://bucket/ecolib", STAC_SOURCE_INFOS
    )

    assert ("running", "run-1", "tehsil") in calls
    assert ("complete", "run-1", "tehsil") in calls


def test_run_descriptor_job_marks_failed_and_reraises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, str, str]] = []

    class FakeProgressStore:
        """Progress store double for failed jobs."""

        def mark_descriptor_running(
            self,
            run_id: str,
            entity: str,
            phase: str,
            layer: str | None = None,
        ) -> None:
            """Record running progress."""
            calls.append(("running", run_id, entity))

        def mark_descriptor_complete(self, run_id: str, entity: str) -> None:
            """Record complete progress."""
            calls.append(("complete", run_id, entity))

        def mark_descriptor_failed(
            self,
            run_id: str,
            entity: str,
            error: str,
        ) -> None:
            """Record failed progress."""
            calls.append(("failed", run_id, entity))

        def set_descriptor_status(self, *args: object, **kwargs: object) -> None:
            """No-op for progress updates."""

    async def fail_pipeline(*args: object, **kwargs: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr("src.pipeline.orchestrator.ProgressStore", FakeProgressStore)
    monkeypatch.setattr("src.pipeline.orchestrator._run_pipeline_stages", fail_pipeline)

    with pytest.raises(RuntimeError, match="boom"):
        run_descriptor_job(
            "run-1", DESCRIPTOR_DATA, "s3://bucket/ecolib", STAC_SOURCE_INFOS
        )

    assert ("running", "run-1", "tehsil") in calls
    assert ("failed", "run-1", "tehsil") in calls


def test_run_pipeline_stages_writes_once_per_resolution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    descriptor = EntityDescriptor.model_validate(
        {
            "entity": "mws",
            "key": "mws_id",
            "geometry": "geometry",
            "partition_by": "river_basin",
            "layers": [
                {
                    "name": "base",
                    "resolution": "static",
                    "stac_item": "base",
                },
                {
                    "name": "terrain",
                    "resolution": "static",
                    "stac_item": "terrain",
                },
            ],
        }
    )
    source_infos = {
        "base": StacSourceInfo(
            stac_item_uri="base",
            columns=set(),
            column_types={},
            crs="EPSG:4326",
            asset_href="s3://bucket/base.parquet",
            source_type="s3",
        ),
        "terrain": StacSourceInfo(
            stac_item_uri="terrain",
            columns=set(),
            column_types={},
            crs="EPSG:4326",
            asset_href="https://example.test/wfs?service=WFS",
            source_type="wfs",
        ),
    }
    writes: list[tuple[str, list[str]]] = []

    class FakeProgress:
        def mark_descriptor_running(self, *args: object, **kwargs: object) -> None:
            pass

    async def fake_fetch(layer, source_info, scratch_dir):
        if layer.name == "base":
            return pl.LazyFrame(
                {
                    "mws_id": ["m1"],
                    "geometry": ["g1"],
                    "river_basin": ["rb1"],
                }
            )
        return pl.LazyFrame({"mws_id": ["m1"], "terrain_cluster": [1]})

    def fake_write(frame, resolution, descriptor, source_info, output_root):
        writes.append((resolution, frame.collect().columns))

    monkeypatch.setattr("src.pipeline.orchestrator.fetch_layer", fake_fetch)
    monkeypatch.setattr("src.pipeline.orchestrator.write_resolution", fake_write)

    asyncio.run(
        _run_pipeline_stages(
            descriptor,
            source_infos,
            "s3://bucket/out",
            workspace_path=str(tmp_path),
            run_id="run-1",
            progress=FakeProgress(),
        )
    )

    assert writes == [
        ("static", ["mws_id", "geometry", "river_basin", "terrain_cluster"])
    ]
