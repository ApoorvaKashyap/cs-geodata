"""Pipeline normalise stage — renames, drops, and type coercions per Descriptor."""

from __future__ import annotations

import polars as pl

from src.descriptor.schema import LayerDescriptor

_STAC_TYPE_MAP: dict[str, pl.PolarsDataType] = {
    "float64": pl.Float64,
    "float32": pl.Float32,
    "float": pl.Float64,
    "double": pl.Float64,
    "int64": pl.Int64,
    "int32": pl.Int32,
    "int16": pl.Int16,
    "int8": pl.Int8,
    "int": pl.Int64,
    "integer": pl.Int64,
    "uint64": pl.UInt64,
    "uint32": pl.UInt32,
    "uint16": pl.UInt16,
    "uint8": pl.UInt8,
    "bool": pl.Boolean,
    "boolean": pl.Boolean,
    "str": pl.Utf8,
    "string": pl.Utf8,
    "utf8": pl.Utf8,
    "date": pl.Date,
    "datetime": pl.Datetime,
    "timestamp": pl.Datetime,
}


def normalise(
    frame: pl.LazyFrame,
    layer: LayerDescriptor,
    column_types: dict[str, str | None] | None = None,
) -> pl.LazyFrame:
    """Apply renames, drops, and STAC-driven type coercions to a LazyFrame.

    Renames are applied first, then drops, then type coercions. Coercions use
    the ``table:columns`` types from the STAC item to ensure numeric columns
    that arrive as strings (e.g. from GeoJSON) are cast to their correct types.

    Args:
        frame: LazyFrame with raw source columns.
        layer: Layer Descriptor carrying rename and drop declarations.
        column_types: Mapping from **source** column name to STAC type string.
            Coercions are applied using post-rename column names. Columns whose
            STAC type is unknown or not in the type map are left unchanged.

    Returns:
        LazyFrame with output column names, without dropped columns, and with
        numeric columns cast to their declared types.
    """
    if layer.rename:
        frame = frame.rename(layer.rename)

    if layer.drop:
        existing = set(frame.collect_schema().names())
        to_drop = [col for col in layer.drop if col in existing]
        if to_drop:
            frame = frame.drop(to_drop)

    if column_types:
        frame = _apply_coercions(frame, layer.rename, column_types)

    return frame


def _apply_coercions(
    frame: pl.LazyFrame,
    rename_map: dict[str, str],
    column_types: dict[str, str | None],
) -> pl.LazyFrame:
    cast_exprs: list[pl.Expr] = []
    schema = frame.collect_schema()

    for source_col, stac_type in column_types.items():
        if stac_type is None:
            continue

        output_col = rename_map.get(source_col, source_col)
        if output_col not in schema.names():
            continue

        polars_type = _STAC_TYPE_MAP.get(stac_type.lower())
        if polars_type is None:
            continue

        current_type = schema[output_col]
        if current_type == polars_type:
            continue

        cast_exprs.append(
            pl.col(output_col).cast(polars_type, strict=False).alias(output_col)
        )

    if cast_exprs:
        frame = frame.with_columns(cast_exprs)

    return frame
