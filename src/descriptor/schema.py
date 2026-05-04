"""Pydantic models for pipeline Descriptor TOML files."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

LayerResolution = Literal["static", "annual", "fortnightly"]


class LayerDescriptor(BaseModel):
    """Describe one source layer that contributes to an entity output.

    Attributes:
        name: Logical layer name used in progress reporting.
        resolution: Output resolution produced from this layer.
        stac_item: URI for the live STAC item JSON.
        rename: Source-to-output column rename declarations.
        drop: Source columns to remove during normalisation.
        temporal_pattern: Pattern used to reshape wide temporal columns.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    resolution: LayerResolution
    stac_item: str
    rename: dict[str, str] = Field(default_factory=dict)
    drop: list[str] = Field(default_factory=list)
    temporal_pattern: str | None = None

    @field_validator("name", "stac_item")
    @classmethod
    def _require_non_empty_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Value must not be empty.")
        return value

    @field_validator("rename")
    @classmethod
    def _require_non_empty_rename_values(cls, value: dict[str, str]) -> dict[str, str]:
        for source, target in value.items():
            if not source.strip() or not target.strip():
                raise ValueError("Rename source and target names must not be empty.")
        return value

    @field_validator("drop")
    @classmethod
    def _require_non_empty_drop_values(cls, value: list[str]) -> list[str]:
        if any(not column.strip() for column in value):
            raise ValueError("Drop column names must not be empty.")
        return value

    @model_validator(mode="after")
    def _validate_temporal_pattern(self) -> LayerDescriptor:
        if self.resolution in {"annual", "fortnightly"} and not self.temporal_pattern:
            raise ValueError("Temporal layers require temporal_pattern.")
        return self


class EntityDescriptor(BaseModel):
    """Describe one ecolib entity dataset to build.

    Attributes:
        entity: ecolib entity name and output directory name.
        key: Primary key column name after rename.
        geometry: Geometry column name after rename.
        partition_by: Static GeoParquet partition column after rename.
        layers: Source layers that produce this entity dataset.
    """

    model_config = ConfigDict(extra="forbid")

    entity: str
    key: str
    geometry: str
    partition_by: str
    layers: list[LayerDescriptor] = Field(min_length=1)

    @field_validator("entity", "key", "geometry", "partition_by")
    @classmethod
    def _require_non_empty_text(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("Value must not be empty.")
        return value

    @model_validator(mode="after")
    def _validate_layer_names(self) -> EntityDescriptor:
        layer_names = [layer.name for layer in self.layers]
        duplicates = sorted(
            {name for name in layer_names if layer_names.count(name) > 1}
        )
        if duplicates:
            raise ValueError(f"Duplicate layer names: {', '.join(duplicates)}.")
        return self

    @classmethod
    def from_toml(cls, content: str) -> Self:
        """Parse a Descriptor from TOML content.

        Args:
            content: Descriptor TOML string.

        Returns:
            Parsed Descriptor model.
        """
        return cls.model_validate(tomllib.loads(content))

    @classmethod
    def from_toml_path(cls, path: str | Path) -> Self:
        """Parse a Descriptor from a local TOML file.

        Args:
            path: Local filesystem path to the Descriptor TOML.

        Returns:
            Parsed Descriptor model.
        """
        return cls.from_toml(Path(path).read_text(encoding="utf-8"))
