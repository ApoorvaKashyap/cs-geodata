import tomllib
from typing import Annotated, Literal

import fsspec  # type: ignore[import-untyped]
from fastapi import Query
from pydantic import BaseModel, Field, model_validator

S3Path = Annotated[str, Query(pattern=r"^s3://([^/]+)/(.*?([^/]+)/?)$")]
LocationField = str | S3Path


class LayerDescriptor(BaseModel):
    """Represents a single ``[[layers]]`` entry from a TOML descriptor file."""

    name: str = Field(description="Unique layer identifier.")
    type: Literal["item", "collection"] = Field(
        description=(
            "'item' = single pan-India file; 'collection' = tehsil-partitioned WFS."
        )
    )
    source: str | None = Field(
        default=None,
        description="Direct file path or S3 URI (used for type='item').",
    )
    stac_item: str | None = Field(
        default=None,
        description="STAC item URL used to infer schema / CRS.",
    )
    sample_item: str | None = Field(
        default=None,
        description="STAC collection URL (used for type='collection').",
    )
    url_template: str | None = Field(
        default=None,
        description=(
            "WFS URL template with {district} and {tehsil} placeholders "
            "(used for type='collection')."
        ),
    )
    drop: list[str] = Field(default_factory=list, description="Columns to drop.")
    rename: dict[str, str] = Field(
        default_factory=dict, description="Column rename mapping."
    )
    resolution: str | None = Field(
        default=None,
        description="Temporal resolution hint, e.g. 'fortnightly'.",
    )


class LayerConversionRequest(BaseModel):
    """Parsed representation of a TOML descriptor file.

    Loaded from a remote TOML file via :func:`load_descriptor` and passed
    through the entire pipeline.
    """

    entity: str = Field(description="The basic unit of the data (e.g. 'mws').")
    key: str = Field(description="Join key shared across all layers.")
    geometry: str = Field(default="geometry", description="Geometry column name.")
    base: str = Field(
        description="Name of the base layer (must match a [[layers]] entry)."
    )
    min_version: float = Field(
        default=0.0, description="Minimum layer version to include (inclusive)."
    )
    max_version: float = Field(
        default=9999.0, description="Maximum layer version to include (inclusive)."
    )
    layer_version: str | None = Field(
        default=None,
        description="URL to the layer version CSV. Optional — omit to skip tehsil filtering and run on all attribute layers without version constraints.",
    )
    output_path: str = Field(
        default="", description="Destination directory for output Parquet files."
    )
    super_layer_source: str | None = Field(
        default=None,
        description=(
            "Path or URI to the super-layer file used for hierarchical partitioning "
            "(e.g. sub-basin boundaries GeoJSON on S3). Optional."
        ),
    )
    super_layer_key: str | None = Field(
        default=None,
        description="Key column in the super-layer file (informational; not used in joins). Optional.",
    )
    super_field: str | None = Field(
        default=None,
        description=(
            "Column name inside the super-layer file whose value is assigned to each "
            "base entity row via a centroid-in-polygon spatial join. Optional."
        ),
    )
    partition_by: str | None = Field(
        default=None,
        description=(
            "Output partition column name. Usually the same as super_field. "
            "All output Parquet files are partitioned on this column. Optional."
        ),
    )
    layers: list[LayerDescriptor] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_super_layer_fields(self) -> "LayerConversionRequest":
        """Ensure super-layer fields are used consistently.

        Rules:
        - ``super_layer_source`` and ``super_field`` must both be provided or
          both be absent. Providing only one is a configuration error.
        - ``partition_by`` is independent and may be set or omitted freely.
          If it is set without a super-layer, the column must already exist
          on the base layer; the pipeline will validate this at runtime.
        """
        has_source = self.super_layer_source is not None
        has_field = self.super_field is not None
        if has_source != has_field:
            missing = "super_field" if has_source else "super_layer_source"
            provided = "super_layer_source" if has_source else "super_field"
            raise ValueError(
                f"'{provided}' was provided but '{missing}' is missing. "
                "Both 'super_layer_source' and 'super_field' must be set together, "
                "or both must be omitted."
            )
        return self

    @property
    def base_layer_descriptor(self) -> LayerDescriptor:
        """Return the descriptor for the configured base layer."""
        for layer in self.layers:
            if layer.name == self.base:
                return layer
        raise ValueError(f"Base layer '{self.base}' not found in layers list.")

    @property
    def attribute_layers(self) -> list[LayerDescriptor]:
        """Return all non-base WFS collection layers."""
        return [
            layer
            for layer in self.layers
            if layer.name != self.base and layer.type == "collection"
        ]


class ConversionRequest(BaseModel):
    """API request payload: a URL pointing to a TOML descriptor and an output path."""

    descriptor_url: str = Field(
        description=(
            "URL to the TOML descriptor file (S3 URI, GitHub raw URL, or plain HTTPS)."
        )
    )
    output_path: str = Field(
        description=(
            "Destination directory path or S3 URI for the output Parquet files."
        )
    )


class BaseLayers(BaseModel):
    base_layer_source: str = Field(
        description="Path or S3 URI to the base layer.",
    )
    output_path: str = Field(
        description="Destination path for the output Parquet file.",
    )
    super_layer_source: str | None = Field(
        default=None,
        description="Optional path or S3 URI to the super-layer file.",
    )
    super_field: str | None = Field(
        default=None,
        description="Optional column name inside the super-layer file.",
    )


def load_descriptor(descriptor_url: str, output_path: str) -> LayerConversionRequest:
    """Fetch and parse a TOML descriptor file into a LayerConversionRequest.

    Supports S3 URIs (via fsspec/s3fs) and plain HTTP/HTTPS URLs.

    Args:
        descriptor_url: URL to the TOML file.
        output_path: Runtime output directory injected into the returned request.

    Returns:
        A fully-validated LayerConversionRequest.

    Raises:
        ValueError: If the descriptor cannot be fetched or parsed.
    """
    try:
        with fsspec.open(descriptor_url, "rb") as fh:
            raw: dict = tomllib.load(fh)
    except Exception as exc:
        raise ValueError(
            f"Failed to load descriptor from {descriptor_url!r}: {exc}"
        ) from exc

    # Normalise field name: TOML uses 'active_locations', model uses 'layer_version'
    if "active_locations" in raw:
        raw["layer_version"] = raw.pop("active_locations")

    raw["output_path"] = output_path

    return LayerConversionRequest.model_validate(raw)
