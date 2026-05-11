"""Tests for pipeline orchestrator jobs."""

from __future__ import annotations

import pytest

from src.pipeline.orchestrator import run_descriptor_job

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
