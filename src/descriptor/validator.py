"""Descriptor validation against STAC item metadata."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence

from pydantic import BaseModel, Field

from src.descriptor.schema import EntityDescriptor, LayerDescriptor
from src.stac.client import extract_table_columns

StacItem = Mapping[str, object]


class DescriptorValidationError(BaseModel):
    """Represent a structured Descriptor validation error.

    Attributes:
        descriptor_uri: URI or path of the Descriptor being validated.
        layer: Layer name when the error belongs to a layer.
        field: Descriptor field that failed validation.
        message: Human-readable validation message.
    """

    descriptor_uri: str | None = None
    layer: str | None = None
    field: str
    message: str


class DescriptorValidationResult(BaseModel):
    """Represent the result of validating one Descriptor.

    Attributes:
        descriptor: Descriptor that was checked.
        errors: Structured validation errors.
    """

    descriptor: EntityDescriptor
    errors: list[DescriptorValidationError] = Field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """Return whether validation passed without errors.

        Returns:
            True when no validation errors were found.
        """
        return not self.errors


def validate_descriptor(
    descriptor: EntityDescriptor,
    stac_items: Mapping[str, StacItem],
    descriptor_uri: str | None = None,
) -> DescriptorValidationResult:
    """Validate a Descriptor against already-fetched STAC item JSON.

    Args:
        descriptor: Parsed Descriptor to validate.
        stac_items: Mapping from Descriptor ``stac_item`` URI to STAC item JSON.
        descriptor_uri: Optional URI or path used in structured errors.

    Returns:
        Structured validation result.
    """
    errors: list[DescriptorValidationError] = []
    layer_output_columns: dict[str, set[str]] = {}

    for layer in descriptor.layers:
        stac_item = stac_items.get(layer.stac_item)
        if stac_item is None:
            errors.append(
                _error(
                    descriptor_uri,
                    layer,
                    "stac_item",
                    "STAC item was not provided for validation.",
                )
            )
            continue

        source_columns = extract_table_columns(stac_item)
        if not source_columns:
            errors.append(
                _error(
                    descriptor_uri,
                    layer,
                    "stac_item",
                    "STAC item does not declare table:columns.",
                )
            )
            continue

        errors.extend(_validate_layer_columns(descriptor_uri, layer, source_columns))
        output_columns = _output_columns_after_normalise(layer, source_columns)
        layer_output_columns[layer.name] = output_columns

        if descriptor.key not in output_columns:
            errors.append(
                _error(
                    descriptor_uri,
                    layer,
                    "key",
                    f"Key column '{descriptor.key}' is not present after rename.",
                )
            )

        if layer.resolution in {"annual", "fortnightly"}:
            errors.extend(
                _validate_temporal_columns(descriptor_uri, layer, source_columns)
            )

    if not any(
        layer.resolution == "static"
        and descriptor.partition_by in layer_output_columns.get(layer.name, set())
        for layer in descriptor.layers
    ):
        errors.append(
            DescriptorValidationError(
                descriptor_uri=descriptor_uri,
                field="partition_by",
                message=(
                    f"Partition column '{descriptor.partition_by}' is not present "
                    "in any static layer after rename."
                ),
            )
        )

    return DescriptorValidationResult(descriptor=descriptor, errors=errors)


def validate_unique_entities(
    descriptors: Sequence[EntityDescriptor],
    descriptor_uris: Sequence[str | None] | None = None,
) -> list[DescriptorValidationError]:
    """Validate that a run does not contain duplicate entity names.

    Args:
        descriptors: Descriptors submitted in one run.
        descriptor_uris: Optional Descriptor URI or path for each Descriptor.

    Returns:
        Structured duplicate-entity errors.
    """
    errors: list[DescriptorValidationError] = []
    seen: dict[str, int] = {}
    uris = descriptor_uris or [None] * len(descriptors)

    for index, descriptor in enumerate(descriptors):
        first_index = seen.get(descriptor.entity)
        if first_index is None:
            seen[descriptor.entity] = index
            continue

        errors.append(
            DescriptorValidationError(
                descriptor_uri=uris[index],
                field="entity",
                message=(
                    f"Duplicate entity '{descriptor.entity}' also appears in "
                    f"Descriptor {uris[first_index] or first_index}."
                ),
            )
        )

    return errors


def _validate_layer_columns(
    descriptor_uri: str | None,
    layer: LayerDescriptor,
    source_columns: set[str],
) -> list[DescriptorValidationError]:
    errors: list[DescriptorValidationError] = []

    for source_column in layer.rename:
        if source_column not in source_columns:
            errors.append(
                _error(
                    descriptor_uri,
                    layer,
                    "rename",
                    f"Rename source column '{source_column}' is not in table:columns.",
                )
            )

    for drop_column in layer.drop:
        if drop_column not in source_columns:
            errors.append(
                _error(
                    descriptor_uri,
                    layer,
                    "drop",
                    f"Drop column '{drop_column}' is not in table:columns.",
                )
            )

    return errors


def _validate_temporal_columns(
    descriptor_uri: str | None,
    layer: LayerDescriptor,
    source_columns: set[str],
) -> list[DescriptorValidationError]:
    if layer.temporal_pattern is None:
        return []

    pattern = temporal_pattern_to_regex(layer.temporal_pattern)
    if any(pattern.fullmatch(column) for column in source_columns):
        return []

    return [
        _error(
            descriptor_uri,
            layer,
            "temporal_pattern",
            "Temporal pattern does not match any STAC table column.",
        )
    ]


def temporal_pattern_to_regex(pattern: str) -> re.Pattern[str]:
    """Convert a Descriptor temporal pattern into a compiled regex.

    Args:
        pattern: Descriptor temporal pattern such as ``{col}_{YYYY}_{YYYY}``.

    Returns:
        Regex that matches source temporal column names.
    """
    token_patterns = {
        "col": r"[A-Za-z_][A-Za-z0-9_]*",
        "YYYY": r"\d{4}",
        "NNN": r"\d{1,3}",
    }
    regex_parts: list[str] = []
    cursor = 0

    for match in re.finditer(r"\{(col|YYYY|NNN)\}", pattern):
        regex_parts.append(re.escape(pattern[cursor : match.start()]))
        token = match.group(1)
        regex_parts.append(token_patterns[token])
        cursor = match.end()

    regex_parts.append(re.escape(pattern[cursor:]))
    return re.compile("".join(regex_parts))


def temporal_pattern_to_col_regex(pattern: str) -> re.Pattern[str]:
    """Convert a Descriptor temporal pattern into a regex that captures the column base name.

    Similar to :func:`temporal_pattern_to_regex` but wraps the ``{col}`` token
    in a capture group so callers can extract the base column name from a match
    via ``match.group(1)``.

    Args:
        pattern: Descriptor temporal pattern such as ``{col}_{YYYY}_{YYYY}``.

    Returns:
        Regex that captures the column base name in group 1 when ``{col}`` is
        present, or matches the full column name otherwise.
    """
    token_patterns = {
        "col": r"([A-Za-z_][A-Za-z0-9_]*)",
        "YYYY": r"\d{4}",
        "NNN": r"\d{1,3}",
    }
    regex_parts: list[str] = []
    cursor = 0

    for match in re.finditer(r"\{(col|YYYY|NNN)\}", pattern):
        regex_parts.append(re.escape(pattern[cursor : match.start()]))
        token = match.group(1)
        regex_parts.append(token_patterns[token])
        cursor = match.end()

    regex_parts.append(re.escape(pattern[cursor:]))
    return re.compile("".join(regex_parts))


def _output_columns_after_normalise(
    layer: LayerDescriptor,
    source_columns: set[str],
) -> set[str]:
    dropped = set(layer.drop)
    output_columns = source_columns - dropped
    for source, target in layer.rename.items():
        if source in output_columns:
            output_columns.remove(source)
            output_columns.add(target)
    return output_columns


def _error(
    descriptor_uri: str | None,
    layer: LayerDescriptor,
    field: str,
    message: str,
) -> DescriptorValidationError:
    return DescriptorValidationError(
        descriptor_uri=descriptor_uri,
        layer=layer.name,
        field=field,
        message=message,
    )
