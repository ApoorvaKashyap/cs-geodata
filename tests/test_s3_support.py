"""Tests for S3 support and credential handling."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.api.routes import _read_descriptor
from src.stac.client import StacClient


def test_read_descriptor_uses_s3_storage_options(monkeypatch) -> None:
    """Verify that _read_descriptor passes credentials to fsspec for S3 URIs."""
    mock_settings = MagicMock()
    mock_settings.aws_access_key_id = "test-key"
    mock_settings.aws_secret_access_key = "test-secret"
    mock_settings.aws_region = "test-region"
    mock_settings.s3_endpoint_url = "http://test-endpoint"

    monkeypatch.setattr("src.utils.s3.get_settings", lambda: mock_settings)

    with patch("fsspec.open") as mock_open:
        mock_file = MagicMock()
        mock_file.__enter__.return_value.read.return_value = 'entity = "tehsil"'
        mock_open.return_value = mock_file

        _read_descriptor("s3://bucket/descriptor.toml")

        mock_open.assert_called_once()
        args, kwargs = mock_open.call_args
        assert args[0] == "s3://bucket/descriptor.toml"
        assert kwargs["key"] == "test-key"
        assert kwargs["secret"] == "test-secret"
        assert kwargs["endpoint_url"] == "http://test-endpoint"


@pytest.mark.anyio
async def test_stac_client_fetches_s3_item(monkeypatch) -> None:
    """Verify that StacClient uses fsspec for S3 items."""
    mock_settings = MagicMock()
    mock_settings.aws_access_key_id = "test-key"
    mock_settings.aws_secret_access_key = "test-secret"
    mock_settings.aws_region = "test-region"
    mock_settings.s3_endpoint_url = None

    monkeypatch.setattr("src.utils.s3.get_settings", lambda: mock_settings)

    with patch("fsspec.open") as mock_open:
        mock_file = MagicMock()
        mock_file.__enter__.return_value.read.return_value = '{"id": "test-item"}'
        mock_open.return_value = mock_file

        client = StacClient()
        item = await client.fetch_item("s3://bucket/item.json")

        assert item == {"id": "test-item"}
        mock_open.assert_called_once()
        args, kwargs = mock_open.call_args
        assert args[0] == "s3://bucket/item.json"
        assert kwargs["key"] == "test-key"


@pytest.mark.anyio
async def test_stac_client_fetch_items_mixed_sources(monkeypatch) -> None:
    """Verify that fetch_items correctly routes S3 and HTTP URIs."""
    client = StacClient()

    async def mock_fetch_s3(uri):
        return {"id": "s3", "uri": uri}

    async def mock_fetch_http(client, uri):
        return {"id": "http", "uri": uri}

    monkeypatch.setattr(client, "_fetch_s3_item", mock_fetch_s3)
    monkeypatch.setattr(client, "_fetch_item_with_client", mock_fetch_http)

    uris = ["s3://bucket/1.json", "https://example.com/2.json", "s3://bucket/3.json"]
    results = await client.fetch_items(uris)

    assert len(results) == 3
    assert results["s3://bucket/1.json"]["id"] == "s3"
    assert results["https://example.com/2.json"]["id"] == "http"
    assert results["s3://bucket/3.json"]["id"] == "s3"
