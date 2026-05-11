"""Tests for pipeline API route helpers."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from fastapi import HTTPException

from src.api.models import RunRequest
from src.api.routes import _fetch_stac_items, _read_descriptor, create_run
from src.descriptor.schema import EntityDescriptor
from src.descriptor.validator import DescriptorValidationError
from src.stac.client import StacClientError


def test_read_descriptor_from_local_path(tmp_path: Path) -> None:
    descriptor_path = tmp_path / "tehsil.toml"
    descriptor_path.write_text(
        """
        entity = "tehsil"
        key = "tehsil_id"
        geometry = "geometry"
        partition_by = "state"

        [[layers]]
        name = "boundaries"
        resolution = "static"
        stac_item = "https://example.test/stac/tehsil.json"
        """,
        encoding="utf-8",
    )

    descriptor, errors = _read_descriptor(str(descriptor_path))

    assert errors == []
    assert descriptor is not None
    assert descriptor.entity == "tehsil"


def test_fetch_stac_items_returns_structured_errors(monkeypatch) -> None:
    descriptor = EntityDescriptor.from_toml(
        """
        entity = "tehsil"
        key = "tehsil_id"
        geometry = "geometry"
        partition_by = "state"

        [[layers]]
        name = "boundaries"
        resolution = "static"
        stac_item = "https://example.test/stac/tehsil.json"
        """
    )

    class FailingStacClient:
        """STAC client double that always fails."""

        async def fetch_item(self, uri: str) -> dict[str, object]:
            """Raise a client error for the requested URI."""
            raise StacClientError(f"failed: {uri}")

    monkeypatch.setattr("src.api.routes.StacClient", FailingStacClient)

    items, source_infos, errors = asyncio.run(
        _fetch_stac_items(descriptor, "descriptor.toml")
    )

    assert items == {}
    assert source_infos == {}
    assert len(errors) == 1
    assert errors[0].field == "stac_item"


def test_create_run_initialises_progress_and_enqueues_jobs(monkeypatch) -> None:
    descriptor = EntityDescriptor.from_toml(
        """
        entity = "tehsil"
        key = "tehsil_id"
        geometry = "geometry"
        partition_by = "state"

        [[layers]]
        name = "boundaries"
        resolution = "static"
        stac_item = "https://example.test/stac/tehsil.json"
        """
    )
    initialised: list[tuple[str, list[str]]] = []
    enqueued: list[tuple[tuple[object, ...], dict[str, object]]] = []

    class FakeProgressStore:
        """Progress store double for run creation."""

        def initialise_run(self, run_id: str, entities: list[str]) -> None:
            """Record run initialisation."""
            initialised.append((run_id, entities))

    class FakeQueue:
        """RQ queue double for run creation."""

        def enqueue(self, *args: object, **kwargs: object) -> None:
            """Record enqueue calls."""
            enqueued.append((args, kwargs))

    class FakeSettings:
        """Settings double for run creation."""

        rq_job_timeout = 123

    async def fake_read_and_validate(
        descriptor_uris: list[str],
    ) -> tuple[list[EntityDescriptor], dict, list[DescriptorValidationError]]:
        return [descriptor], {}, []

    monkeypatch.setattr("src.api.routes.ProgressStore", FakeProgressStore)
    monkeypatch.setattr("src.api.routes.get_runs_queue", lambda: FakeQueue())
    monkeypatch.setattr("src.api.routes.get_settings", lambda: FakeSettings())
    monkeypatch.setattr(
        "src.api.routes._read_and_validate_descriptors",
        fake_read_and_validate,
    )

    response = asyncio.run(
        create_run(
            RunRequest(
                descriptors=["descriptor.toml"],
                output_root="s3://bucket/ecolib",
            )
        )
    )

    assert response.run_id
    assert initialised == [(response.run_id, ["tehsil"])]
    assert len(enqueued) == 1
    assert enqueued[0][0][1] == response.run_id


def test_create_run_rejects_validation_errors(monkeypatch) -> None:
    async def fake_read_and_validate(
        descriptor_uris: list[str],
    ) -> tuple[list[EntityDescriptor], dict, list[DescriptorValidationError]]:
        return (
            [],
            {},
            [
                DescriptorValidationError(
                    descriptor_uri="bad.toml",
                    field="descriptor",
                    message="invalid",
                )
            ],
        )

    monkeypatch.setattr(
        "src.api.routes._read_and_validate_descriptors",
        fake_read_and_validate,
    )

    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(
            create_run(
                RunRequest(
                    descriptors=["bad.toml"],
                    output_root="s3://bucket/ecolib",
                )
            )
        )

    assert exc_info.value.status_code == 422
