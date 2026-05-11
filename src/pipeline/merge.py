"""Pipeline merge stage for entity-shaped outputs."""

from __future__ import annotations

import polars as pl

from src.descriptor.schema import LayerResolution


def merge_resolution_frames(
    frames: list[pl.LazyFrame],
    resolution: LayerResolution,
    key: str,
) -> pl.LazyFrame:
    """Merge processed layer frames into one entity output frame.

    Args:
        frames: Processed frames for one output resolution.
        resolution: Output resolution being merged.
        key: Entity primary key column.

    Returns:
        Merged LazyFrame for the output resolution.

    Raises:
        RuntimeError: If no frames are provided or a required merge key is missing.
    """
    if not frames:
        raise RuntimeError(f"No frames provided for {resolution} merge.")

    join_keys = _join_keys(resolution, key)
    merged = frames[0]
    _validate_keys(merged, join_keys, resolution)

    for frame in frames[1:]:
        _validate_keys(frame, join_keys, resolution)
        right = _drop_overlapping_columns(merged, frame, join_keys)
        merged = merged.join(right, on=join_keys, how="left")

    return merged


def _join_keys(resolution: LayerResolution, key: str) -> list[str]:
    if resolution == "static":
        return [key]
    if resolution == "annual":
        return [key, "year"]
    return [key, "date"]


def _validate_keys(
    frame: pl.LazyFrame,
    join_keys: list[str],
    resolution: LayerResolution,
) -> None:
    columns = set(frame.collect_schema().names())
    missing = [col for col in join_keys if col not in columns]
    if missing:
        raise RuntimeError(
            f"{resolution} frame is missing required merge columns: "
            f"{', '.join(missing)}."
        )


def _drop_overlapping_columns(
    left: pl.LazyFrame,
    right: pl.LazyFrame,
    join_keys: list[str],
) -> pl.LazyFrame:
    left_columns = set(left.collect_schema().names())
    right_columns = right.collect_schema().names()
    overlap = [
        column
        for column in right_columns
        if column in left_columns and column not in join_keys
    ]
    if not overlap:
        return right
    return right.drop(overlap)
