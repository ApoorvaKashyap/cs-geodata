"""Per-tehsil RQ fetch job — runs on the fetch worker queue."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import polars as pl

from src.utils.configs import get_settings


def fetch_tehsil_job(
    url: str,
    output_path: str,
) -> None:
    """Fetch one tehsil's GeoJSON from GeoServer and write it as Parquet.

    This is the unit of work dispatched onto the ``fetch`` RQ queue. One job
    is enqueued per tehsil per WFS layer. RQ's built-in Redis tracking records
    job status automatically — no manual progress updates required.

    The output Parquet file is written to the shared run workspace so the runs
    worker can read and concatenate it after all fetch jobs complete.

    Args:
        url: Full GeoServer WFS URL for one tehsil layer.
        output_path: Absolute path to write the output Parquet file.
    """
    settings = get_settings()

    try:
        response = httpx.get(url, timeout=settings.wfs_request_timeout_seconds)
        response.raise_for_status()
        data = response.json()
    except httpx.HTTPError as exc:
        raise RuntimeError(f"WFS fetch failed for '{url}': {exc}") from exc
    except ValueError as exc:
        raise RuntimeError(f"WFS response for '{url}' was not valid JSON.") from exc

    features = data.get("features") or []
    if not features:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame().write_parquet(output_path)
        return

    rows: list[dict[str, object]] = []
    for feature in features:
        row: dict[str, object] = dict(feature.get("properties") or {})
        geometry = feature.get("geometry")
        if geometry is not None:
            row["geometry"] = json.dumps(geometry)
        rows.append(row)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(out)
