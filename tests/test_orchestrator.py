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


def test_run_descriptor_job_marks_running_and_complete(monkeypatch) -> None:
    calls: list[tuple[str, str, str | None]] = []

    class FakeProgressStore:
        """Progress store double for successful jobs."""

        def mark_descriptor_running(
            self,
            run_id: str,
            entity: str,
            phase: str,
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

    monkeypatch.setattr("src.pipeline.orchestrator.ProgressStore", FakeProgressStore)

    run_descriptor_job("run-1", DESCRIPTOR_DATA, "s3://bucket/ecolib")

    assert calls == [("running", "run-1", "tehsil"), ("complete", "run-1", "tehsil")]


def test_run_descriptor_job_marks_failed_and_reraises(monkeypatch) -> None:
    calls: list[tuple[str, str, str]] = []

    class FakeProgressStore:
        """Progress store double for failed jobs."""

        def mark_descriptor_running(
            self,
            run_id: str,
            entity: str,
            phase: str,
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

    def fail_pipeline(*args: object) -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr("src.pipeline.orchestrator.ProgressStore", FakeProgressStore)
    monkeypatch.setattr("src.pipeline.orchestrator._run_pipeline_stages", fail_pipeline)

    with pytest.raises(RuntimeError, match="boom"):
        run_descriptor_job("run-1", DESCRIPTOR_DATA, "s3://bucket/ecolib")

    assert calls == [("running", "run-1", "tehsil"), ("failed", "run-1", "tehsil")]
