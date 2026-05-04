"""Tests for Redis-backed progress store behavior."""

from __future__ import annotations

from collections import defaultdict

from src.progress.store import ProgressStore


class FakeRedis:
    """Small Redis subset used by ProgressStore tests."""

    def __init__(self) -> None:
        """Initialize in-memory Redis data structures."""
        self.values: dict[str, str] = {}
        self.lists: dict[str, list[str]] = defaultdict(list)
        self.expirations: dict[str, int] = {}

    def delete(self, key: str) -> None:
        """Delete a key from the fake store."""
        self.values.pop(key, None)
        self.lists.pop(key, None)

    def rpush(self, key: str, value: str) -> None:
        """Append a value to a fake Redis list."""
        self.lists[key].append(value)

    def expire(self, key: str, seconds: int) -> None:
        """Record a key expiration."""
        self.expirations[key] = seconds

    def setex(self, key: str, seconds: int, value: str) -> None:
        """Set a string value with expiration."""
        self.values[key] = value
        self.expirations[key] = seconds

    def lrange(self, key: str, start: int, end: int) -> list[str]:
        """Return values from a fake Redis list."""
        values = self.lists.get(key, [])
        return values[start:] if end == -1 else values[start : end + 1]

    def get(self, key: str) -> str | None:
        """Return a fake Redis string value."""
        return self.values.get(key)

    def exists(self, key: str) -> int:
        """Return whether a key exists."""
        return int(key in self.values or key in self.lists)


def make_store() -> ProgressStore:
    store = ProgressStore.__new__(ProgressStore)
    store.redis = FakeRedis()
    store.ttl_seconds = 60
    return store


def test_initialise_run_deduplicates_entities() -> None:
    store = make_store()

    progress = store.initialise_run("run-1", ["mws", "mws", "tehsil"])

    assert progress.run_id == "run-1"
    assert progress.status == "pending"
    assert [descriptor.entity for descriptor in progress.descriptors] == [
        "mws",
        "tehsil",
    ]
    assert store.has_run("run-1")


def test_run_status_aggregates_descriptor_statuses() -> None:
    store = make_store()
    store.initialise_run("run-1", ["mws", "tehsil"])

    store.mark_descriptor_running("run-1", "mws", phase="fetch", layer="terrain")
    assert store.get_run("run-1").status == "running"

    store.mark_descriptor_complete("run-1", "mws")
    store.mark_descriptor_complete("run-1", "tehsil")
    assert store.get_run("run-1").status == "complete"

    store.mark_descriptor_failed("run-1", "mws", "boom")
    assert store.get_run("run-1").status == "failed"
