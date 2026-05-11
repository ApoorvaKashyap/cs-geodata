"""STAC catalogue hierarchy walker for WFS tehsil enumeration."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx

from src.utils.configs import get_settings


@dataclass(frozen=True)
class TehsilKey:
    """Represent one tehsil location tuple from the STAC hierarchy.

    Attributes:
        state: State name as it appears in the STAC hierarchy.
        district: District name as it appears in the STAC hierarchy.
        tehsil: Tehsil name as it appears in the STAC hierarchy.
    """

    state: str
    district: str
    tehsil: str


async def walk_stac_tehsil_hierarchy(
    stac_item_uri: str,
    timeout_seconds: int | None = None,
    max_concurrent_fetches: int | None = None,
) -> list[TehsilKey]:
    """Walk the CoreStack STAC catalogue to enumerate tehsil location tuples.

    The catalogue is structured as::

        tehsil_wise/{state}/{district}/{tehsil}/

    This function derives the catalogue root from the ``stac_item`` URI and
    walks its child links to produce the complete tehsil iteration list.

    Args:
        stac_item_uri: STAC item URI used to derive the catalogue root.
        timeout_seconds: HTTP timeout in seconds. Defaults to
            ``stac_request_timeout_seconds`` from settings.
        max_concurrent_fetches: Maximum concurrent catalogue GET requests.
            Defaults to ``wfs_max_concurrent_fetches`` from settings.

    Returns:
        List of tehsil location tuples in the order the catalogue returns them.

    Raises:
        RuntimeError: If the catalogue root cannot be fetched.
    """
    settings = get_settings()
    timeout = (
        timeout_seconds
        if timeout_seconds is not None
        else settings.stac_request_timeout_seconds
    )
    max_concurrency = (
        max_concurrent_fetches
        if max_concurrent_fetches is not None
        else settings.wfs_max_concurrent_fetches
    )
    catalogue_root = _derive_catalogue_root(stac_item_uri)
    semaphore = asyncio.Semaphore(max(1, max_concurrency))

    async with httpx.AsyncClient(timeout=timeout) as client:
        return await _walk_root(client, catalogue_root, semaphore)


def _derive_catalogue_root(stac_item_uri: str) -> str:
    marker = "tehsil_wise/"
    idx = stac_item_uri.find(marker)
    if idx != -1:
        return stac_item_uri[: idx + len(marker)]
    parts = stac_item_uri.rstrip("/").rsplit("/", 1)
    return parts[0] + "/"


async def _walk_root(
    client: httpx.AsyncClient,
    root_url: str,
    semaphore: asyncio.Semaphore,
) -> list[TehsilKey]:
    try:
        response = await _get(client, root_url, semaphore)
        response.raise_for_status()
        root = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise RuntimeError(
            f"Failed to fetch STAC hierarchy root '{root_url}': {exc}"
        ) from exc

    state_links = _child_links(root)
    if not state_links:
        _log_warning(f"No state-level child links found at '{root_url}'.")
        return []

    tasks = [_walk_state(client, root_url, name, semaphore) for name in state_links]
    nested = await asyncio.gather(*tasks)
    return [key for keys in nested for key in keys]


async def _walk_state(
    client: httpx.AsyncClient,
    root_url: str,
    state: str,
    semaphore: asyncio.Semaphore,
) -> list[TehsilKey]:
    url = root_url + state + "/"
    try:
        response = await _get(client, url, semaphore)
        response.raise_for_status()
        doc = response.json()
    except (httpx.HTTPError, ValueError):
        _log_warning(f"Failed to fetch state catalogue '{url}'; skipping.")
        return []

    district_links = _child_links(doc)
    tasks = [
        _walk_district(client, url, state, name, semaphore) for name in district_links
    ]
    nested = await asyncio.gather(*tasks)
    return [key for keys in nested for key in keys]


async def _walk_district(
    client: httpx.AsyncClient,
    state_url: str,
    state: str,
    district: str,
    semaphore: asyncio.Semaphore,
) -> list[TehsilKey]:
    url = state_url + district + "/"
    try:
        response = await _get(client, url, semaphore)
        response.raise_for_status()
        doc = response.json()
    except (httpx.HTTPError, ValueError):
        _log_warning(f"Failed to fetch district catalogue '{url}'; skipping.")
        return []

    tehsil_links = _child_links(doc)
    return [TehsilKey(state=state, district=district, tehsil=t) for t in tehsil_links]


async def _get(
    client: httpx.AsyncClient,
    url: str,
    semaphore: asyncio.Semaphore,
) -> httpx.Response:
    async with semaphore:
        return await client.get(url)


def _child_links(doc: object) -> list[str]:
    if not isinstance(doc, dict):
        return []
    links = doc.get("links") or []
    names: list[str] = []
    for link in links:
        if not isinstance(link, dict):
            continue
        rel = link.get("rel")
        if rel not in {"child", "item"}:
            continue
        href = link.get("href", "")
        name = href.rstrip("/").rsplit("/", 1)[-1]
        if name:
            names.append(name)
    return names


def _log_warning(message: str) -> None:
    try:
        from loguru import logger

        logger.warning(message)
    except Exception:
        pass
