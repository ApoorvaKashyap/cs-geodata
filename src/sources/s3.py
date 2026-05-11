"""S3Source — single Pan-India file download from S3."""

from __future__ import annotations

from pathlib import Path

import fsspec
import polars as pl

from src.sources.base import AbstractSource


class S3Source(AbstractSource):
    """Download and read one Pan-India file from S3.

    Supports GeoJSON and Parquet files. The file is downloaded to a local
    scratch directory before reading so that Polars can stream it without
    holding the S3 connection open.

    Attributes:
        asset_href: S3 URI of the source file (``s3://bucket/key``).
        scratch_dir: Local directory to write the downloaded file.
    """

    def __init__(self, asset_href: str, scratch_dir: Path) -> None:
        """Initialise the S3Source.

        Args:
            asset_href: S3 URI of the source file.
            scratch_dir: Local directory for the temporary download.
        """
        self.asset_href = asset_href
        self.scratch_dir = scratch_dir

    async def fetch(self) -> pl.LazyFrame:
        """Download the S3 file and return it as a LazyFrame.

        Returns:
            LazyFrame read from the downloaded file.

        Raises:
            RuntimeError: If the file cannot be downloaded or parsed.
        """
        local_path = self._download()
        return _read_file(local_path)

    def _download(self) -> Path:
        from src.utils.s3 import get_s3_storage_options

        storage_options = get_s3_storage_options()

        suffix = Path(self.asset_href.split("?")[0]).suffix.lower()
        local_path = self.scratch_dir / f"source{suffix}"

        try:
            with (
                fsspec.open(self.asset_href, mode="rb", **storage_options) as src,
                local_path.open("wb") as dst,
            ):
                dst.write(src.read())
        except Exception as exc:
            raise RuntimeError(
                f"Failed to download S3 asset '{self.asset_href}': {exc}"
            ) from exc

        return local_path


def _read_file(path: Path) -> pl.LazyFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pl.scan_parquet(path)
    if suffix in {".geojson", ".json"}:
        return _geojson_to_lazyframe(path)
    raise RuntimeError(f"Unsupported source file type: {suffix}")


def _geojson_to_lazyframe(path: Path) -> pl.LazyFrame:
    import json

    raw = json.loads(path.read_text(encoding="utf-8"))
    features = raw.get("features", [])
    if not features:
        raise RuntimeError(f"GeoJSON at '{path}' contains no features.")

    rows: list[dict[str, object]] = []
    for feature in features:
        row: dict[str, object] = dict(feature.get("properties") or {})
        geometry = feature.get("geometry")
        if geometry is not None:
            row["geometry"] = json.dumps(geometry)
        rows.append(row)

    return pl.LazyFrame(rows)


def _clean(options: dict[str, str | None]) -> dict[str, object]:
    return {k: v for k, v in options.items() if v is not None}
