"""Tests for pipeline fetch stage helpers."""

from __future__ import annotations

from pathlib import Path

from rq.job import JobStatus

from src.pipeline.fetch import _wait_for_jobs


class FakeJobRef:
    """Minimal queued job reference used by fetch-stage tests."""

    def __init__(self, job_id: str) -> None:
        """Initialize the fake job reference."""
        self.id = job_id


class FakeFetchedJob:
    """Fetched job object that returns a configured status."""

    def __init__(self, status: JobStatus) -> None:
        """Initialize the fetched job."""
        self.status = status

    def get_status(self) -> JobStatus:
        """Return the configured status."""
        return self.status


def test_wait_for_jobs_only_polls_pending_jobs(monkeypatch) -> None:
    status_sequences = {
        "a": [JobStatus.FINISHED],
        "b": [JobStatus.QUEUED, JobStatus.FINISHED],
    }
    fetch_counts = {"a": 0, "b": 0}

    def fake_fetch(job_id: str, connection: object) -> FakeFetchedJob:
        fetch_counts[job_id] += 1
        sequence = status_sequences[job_id]
        status = sequence.pop(0) if len(sequence) > 1 else sequence[0]
        return FakeFetchedJob(status)

    monkeypatch.setattr("src.pipeline.fetch.Job.fetch", fake_fetch)
    monkeypatch.setattr("src.pipeline.fetch.time.sleep", lambda seconds: None)

    failed = _wait_for_jobs(
        [(FakeJobRef("a"), Path("a.parquet")), (FakeJobRef("b"), Path("b.parquet"))],
        redis=object(),
    )

    assert failed == set()
    assert fetch_counts == {"a": 1, "b": 2}


def test_wait_for_jobs_returns_failed_job_ids(monkeypatch) -> None:
    def fake_fetch(job_id: str, connection: object) -> FakeFetchedJob:
        status = JobStatus.FAILED if job_id == "bad" else JobStatus.FINISHED
        return FakeFetchedJob(status)

    monkeypatch.setattr("src.pipeline.fetch.Job.fetch", fake_fetch)

    failed = _wait_for_jobs(
        [
            (FakeJobRef("ok"), Path("ok.parquet")),
            (FakeJobRef("bad"), Path("bad.parquet")),
        ],
        redis=object(),
    )

    assert failed == {"bad"}
