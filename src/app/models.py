from typing import Annotated, Literal

from fastapi import Query
from pydantic import BaseModel, Field

S3Path = Annotated[str, Query(pattern=r"^s3://([^/]+)/(.*?([^/]+)/?)$")]
LocationField = str | S3Path  # local path string or s3:// URI


class ColumnMapping(BaseModel):
    drop_columns: list[str] = Field(default_factory=list)
    rename_columns: dict[str, str] = Field(default_factory=dict)
    # Maps column name -> split strategy ("split-min-max", "only-max", etc.)
    column_changes: dict[str, str] = Field(default_factory=dict)


class LayerConversionRequest(BaseModel):
    unit: Literal["mws", "farms", "forests"] = Field(
        description="The basic unit of the data."
    )
    # Maps layer label -> file path or s3:// URI
    layers: list[str] = Field(
        description="List of all associated layers.",
        default_factory=list,
    )
    output_path: LocationField = Field(
        description="Where to write the output Parquet file."
    )
    column_map: dict[str, ColumnMapping] = Field(
        description="Per-layer column operations (drop, rename, split).",
        default_factory=dict,
    )
    common_cols: list[str] = Field(
        description="Columns shared across all layers (never prefixed).",
        default_factory=list,
    )
    base_layer: dict[str, str] = Field(
        description="Label & path of the layer whose global columns anchor the merge.",
        default_factory=dict,
    )
    key: str = Field(
        description="Join key shared across all layers.",
        default="uid",
    )
    parquet_version: float = Field(default=1.0)
    use_prev_mapping: bool = Field(default=False)
    layer_version: str = Field(description="Version of the layer data.", default="")


class BaseLayers(BaseModel):
    layers: dict[Literal["mws", "farms", "forests"], LocationField] = Field(
        description="Filetpath to base layers.",
        default_factory=dict,
    )
