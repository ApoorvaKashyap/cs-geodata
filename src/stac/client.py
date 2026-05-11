"""Client helpers for live STAC item reads."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import urlparse

import httpx

from src.utils.configs import get_settings

StacItem = dict[str, Any]
SourceType = Literal["wfs", "s3"]


class StacClientError(RuntimeError):
    """Raised when a STAC item cannot be fetched or interpreted."""


@dataclass(frozen=True)
class StacSourceInfo:
    """Represent source metadata resolved from a STAC item.

    Attributes:
        stac_item_uri: URI used to fetch the STAC item.
        columns: Source table column names from ``table:columns``.
        column_types: Source table column types keyed by column name.
        crs: CRS code from ``proj:code``.
        asset_href: Source asset URI or URL.
        source_type: Source type inferred from ``asset_href``.
    """

    stac_item_uri: str
    columns: set[str]
    column_types: dict[str, str | None]
    crs: str | None
    asset_href: str
    source_type: SourceType


class StacClient:
    """Fetch live STAC items and resolve source metadata."""

    def __init__(self, timeout_seconds: int | None = None) -> None:
        """Initialize the STAC client.

        Args:
            timeout_seconds: HTTP timeout in seconds. Defaults to configured
                ``STAC_REQUEST_TIMEOUT_SECONDS``.
        """
        self.timeout_seconds = (
            timeout_seconds
            if timeout_seconds is not None
            else get_settings().stac_request_timeout_seconds
        )

    async def fetch_item(self, uri: str) -> StacItem:
        """Fetch one STAC item JSON document.

        Args:
            uri: HTTP/HTTPS or S3 URI for a STAC item JSON document.

        Returns:
            Parsed STAC item JSON.

        Raises:
            StacClientError: If the item cannot be fetched or is not JSON.
        """
        if uri.startswith("s3://"):
            return await self._fetch_s3_item(uri)
        return await self._fetch_http_item(uri)

    async def _fetch_http_item(self, uri: str) -> StacItem:
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.get(uri)
                response.raise_for_status()
                item = response.json()
        except httpx.HTTPError as exc:
            raise StacClientError(f"Failed to fetch STAC item '{uri}': {exc}") from exc
        except ValueError as exc:
            raise StacClientError(f"STAC item '{uri}' did not return JSON.") from exc

        if not isinstance(item, dict):
            raise StacClientError(f"STAC item '{uri}' JSON must be an object.")
        return item

    async def _fetch_s3_item(self, uri: str) -> StacItem:
        import json

        import fsspec

        from src.utils.s3 import get_s3_storage_options

        try:
            storage_options = get_s3_storage_options()
            with fsspec.open(uri, mode="rt", encoding="utf-8", **storage_options) as f:
                item = json.loads(f.read())
        except Exception as exc:
            raise StacClientError(
                f"Failed to fetch S3 STAC item '{uri}': {exc}"
            ) from exc

        if not isinstance(item, dict):
            raise StacClientError(f"STAC item '{uri}' JSON must be an object.")
        return item

    async def fetch_items(self, uris: Iterable[str]) -> dict[str, StacItem]:
        """Fetch multiple STAC item JSON documents.

        Args:
            uris: STAC item URIs to fetch.

        Returns:
            Mapping from URI to parsed STAC item JSON.
        """
        unique_uris = list(dict.fromkeys(uris))
        s3_uris = [u for u in unique_uris if u.startswith("s3://")]
        http_uris = [u for u in unique_uris if not u.startswith("s3://")]

        results: dict[str, StacItem] = {}

        # Fetch S3 items in parallel
        if s3_uris:
            s3_results = await asyncio.gather(
                *[self._fetch_s3_item(u) for u in s3_uris]
            )
            results.update(zip(s3_uris, s3_results, strict=True))

        # Fetch HTTP items using a shared client
        if http_uris:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                http_results = await asyncio.gather(
                    *[self._fetch_item_with_client(client, u) for u in http_uris]
                )
                results.update(zip(http_uris, http_results, strict=True))

        return results

    async def fetch_source_infos(
        self, uris: Iterable[str]
    ) -> dict[str, StacSourceInfo]:
        """Fetch STAC items and resolve source metadata for each URI.

        Args:
            uris: STAC item URIs to fetch.

        Returns:
            Mapping from URI to resolved source metadata.
        """
        items = await self.fetch_items(uris)
        return {uri: self.resolve_source_info(uri, item) for uri, item in items.items()}

    def resolve_source_info(self, uri: str, item: StacItem) -> StacSourceInfo:
        """Resolve schema, CRS, and asset source metadata from a STAC item.

        Args:
            uri: URI used to fetch the STAC item.
            item: Parsed STAC item JSON.

        Returns:
            Source metadata required by validation and pipeline stages.

        Raises:
            StacClientError: If the item lacks a usable asset href.
        """
        asset_href = extract_asset_href(item)
        if asset_href is None:
            raise StacClientError(f"STAC item '{uri}' does not declare an asset href.")

        return StacSourceInfo(
            stac_item_uri=uri,
            columns=extract_table_columns(item),
            column_types=extract_table_column_types(item),
            crs=extract_proj_code(item),
            asset_href=asset_href,
            source_type=infer_source_type(asset_href),
        )

    async def _fetch_item_with_client(
        self, client: httpx.AsyncClient, uri: str
    ) -> StacItem:
        try:
            response = await client.get(uri)
            response.raise_for_status()
            item = response.json()
        except httpx.HTTPError as exc:
            raise StacClientError(f"Failed to fetch STAC item '{uri}': {exc}") from exc
        except ValueError as exc:
            raise StacClientError(f"STAC item '{uri}' did not return JSON.") from exc

        if not isinstance(item, dict):
            raise StacClientError(f"STAC item '{uri}' JSON must be an object.")
        return item


def extract_table_columns(item: Mapping[str, Any]) -> set[str]:
    """Extract source column names from a STAC item.

    Args:
        item: Parsed STAC item JSON.

    Returns:
        Column names declared under ``table:columns``.
    """
    columns = _get_table_columns(item)
    if not isinstance(columns, list):
        return set()

    names: set[str] = set()
    for column in columns:
        if isinstance(column, Mapping):
            name = column.get("name")
            if isinstance(name, str):
                names.add(name)
        elif isinstance(column, str):
            names.add(column)
    return names


def extract_table_column_types(item: Mapping[str, Any]) -> dict[str, str | None]:
    """Extract source column types from a STAC item.

    Args:
        item: Parsed STAC item JSON.

    Returns:
        Column type values keyed by column name.
    """
    columns = _get_table_columns(item)
    if not isinstance(columns, list):
        return {}

    types: dict[str, str | None] = {}
    for column in columns:
        if not isinstance(column, Mapping):
            continue
        name = column.get("name")
        if not isinstance(name, str):
            continue

        column_type = column.get("type")
        types[name] = column_type if isinstance(column_type, str) else None
    return types


def extract_proj_code(item: Mapping[str, Any]) -> str | None:
    """Extract the CRS code from a STAC item.

    Args:
        item: Parsed STAC item JSON.

    Returns:
        CRS code from ``proj:code`` when present.
    """
    proj_code = item.get("proj:code")
    if proj_code is None:
        properties = item.get("properties")
        if isinstance(properties, Mapping):
            proj_code = properties.get("proj:code")
    return proj_code if isinstance(proj_code, str) else None


def extract_asset_href(item: Mapping[str, Any]) -> str | None:
    """Extract the primary asset href from a STAC item.

    Args:
        item: Parsed STAC item JSON.

    Returns:
        Preferred asset href when present.
    """
    assets = item.get("assets")
    if not isinstance(assets, Mapping):
        return None

    preferred_asset_keys = ("data", "source", "asset")

    for asset in assets.values():
        if not isinstance(asset, Mapping):
            continue
        roles = asset.get("roles")
        href = asset.get("href")
        if isinstance(href, str) and isinstance(roles, list) and "data" in roles:
            return href

    for asset_key in preferred_asset_keys:
        asset = assets.get(asset_key)
        if isinstance(asset, Mapping) and isinstance(asset.get("href"), str):
            return asset["href"]

    for asset in assets.values():
        if isinstance(asset, Mapping) and isinstance(asset.get("href"), str):
            return asset["href"]
    return None


def infer_source_type(asset_href: str) -> SourceType:
    """Infer pipeline source type from a STAC asset href.

    Args:
        asset_href: Asset URI or URL from a STAC item.

    Returns:
        Source type used by the acquisition layer.

    Raises:
        StacClientError: If the href does not map to a supported source type.
    """
    parsed = urlparse(asset_href)
    if parsed.scheme == "s3":
        return "s3"
    if (
        "service=wfs" in asset_href.lower()
        or "request=getfeature" in asset_href.lower()
    ):
        return "wfs"
    raise StacClientError(f"Unsupported STAC asset href source type: {asset_href}")


def _get_table_columns(item: Mapping[str, Any]) -> object:
    columns = item.get("table:columns")
    if columns is None:
        properties = item.get("properties")
        if isinstance(properties, Mapping):
            columns = properties.get("table:columns")
    return columns
