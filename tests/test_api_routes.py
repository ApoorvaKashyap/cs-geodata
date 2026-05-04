"""Tests for pipeline API route helpers."""

from __future__ import annotations

import asyncio
from pathlib import Path

from src.api.routes import _fetch_stac_items, _read_descriptor
from src.descriptor.schema import EntityDescriptor
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

    items, errors = asyncio.run(_fetch_stac_items(descriptor, "descriptor.toml"))

    assert items == {}
    assert len(errors) == 1
    assert errors[0].field == "stac_item"
