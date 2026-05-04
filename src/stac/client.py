"""Client helpers for live STAC item reads."""

from __future__ import annotations

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
        crs: CRS code from ``proj:code``.
        asset_href: Source asset URI or URL.
        source_type: Source type inferred from ``asset_href``.
    """

    stac_item_uri: str
    columns: set[str]
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
        settings = get_settings()
        self.timeout_seconds = timeout_seconds or settings.stac_request_timeout_seconds

    async def fetch_item(self, uri: str) -> StacItem:
        """Fetch one STAC item JSON document.

        Args:
            uri: HTTP or HTTPS URI for a STAC item JSON document.

        Returns:
            Parsed STAC item JSON.

        Raises:
            StacClientError: If the item cannot be fetched or is not JSON.
        """
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

    async def fetch_items(self, uris: Iterable[str]) -> dict[str, StacItem]:
        """Fetch multiple STAC item JSON documents.

        Args:
            uris: STAC item URIs to fetch.

        Returns:
            Mapping from URI to parsed STAC item JSON.
        """
        unique_uris = dict.fromkeys(uris)
        items: dict[str, StacItem] = {}
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            for uri in unique_uris:
                items[uri] = await self._fetch_item_with_client(client, uri)
        return items

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
    columns = item.get("table:columns")
    if columns is None:
        properties = item.get("properties")
        if isinstance(properties, Mapping):
            columns = properties.get("table:columns")

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

    for asset in assets.values():
        if not isinstance(asset, Mapping):
            continue
        roles = asset.get("roles")
        href = asset.get("href")
        if isinstance(href, str) and isinstance(roles, list) and "data" in roles:
            return href

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
