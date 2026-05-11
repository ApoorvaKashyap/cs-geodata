"""Tests for Descriptor schema and validation."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from src.descriptor.schema import EntityDescriptor
from src.descriptor.validator import validate_descriptor, validate_unique_entities


def test_descriptor_parses_static_layer_from_toml() -> None:
    descriptor = EntityDescriptor.from_toml(
        """
        entity = "tehsil"
        key = "tehsil_id"
        geometry = "geometry"
        partition_by = "state"

        [[layers]]
        name = "boundaries"
        resolution = "static"
        stac_item = "https://example.test/stac/tehsil.json"

        [layers.rename]
        uid = "tehsil_id"
        STATE = "state"
        """
    )

    assert descriptor.entity == "tehsil"
    assert descriptor.layers[0].rename == {"uid": "tehsil_id", "STATE": "state"}


def test_temporal_layer_requires_temporal_pattern() -> None:
    with pytest.raises(ValidationError):
        EntityDescriptor.from_toml(
            """
            entity = "mws"
            key = "mws_id"
            geometry = "geometry"
            partition_by = "river_basin"

            [[layers]]
            name = "cropping_intensity"
            resolution = "annual"
            stac_item = "https://example.test/stac/ci.json"
            """
        )


def test_descriptor_validation_uses_columns_after_rename() -> None:
    descriptor = EntityDescriptor.from_toml(
        """
        entity = "tehsil"
        key = "tehsil_id"
        geometry = "geometry"
        partition_by = "state"

        [[layers]]
        name = "boundaries"
        resolution = "static"
        stac_item = "https://example.test/stac/tehsil.json"

        [layers.rename]
        uid = "tehsil_id"
        STATE = "state"
        """
    )
    stac_item = {
        "table:columns": [
            {"name": "uid"},
            {"name": "STATE"},
            {"name": "geometry"},
        ]
    }

    result = validate_descriptor(
        descriptor,
        {"https://example.test/stac/tehsil.json": stac_item},
        descriptor_uri="descriptor.toml",
    )

    assert result.is_valid
    assert result.errors == []


def test_descriptor_validation_reports_missing_columns() -> None:
    descriptor = EntityDescriptor.from_toml(
        """
        entity = "tehsil"
        key = "tehsil_id"
        geometry = "geometry"
        partition_by = "state"

        [[layers]]
        name = "boundaries"
        resolution = "static"
        stac_item = "https://example.test/stac/tehsil.json"
        drop = ["missing_drop"]

        [layers.rename]
        missing_uid = "tehsil_id"
        """
    )
    stac_item = {"table:columns": [{"name": "uid"}, {"name": "STATE"}]}

    result = validate_descriptor(
        descriptor,
        {"https://example.test/stac/tehsil.json": stac_item},
    )

    fields = {error.field for error in result.errors}
    assert {"rename", "drop", "key", "partition_by"} <= fields


def test_validate_unique_entities_reports_duplicates() -> None:
    descriptor = EntityDescriptor.from_toml(
        """
        entity = "tehsil"
        key = "tehsil_id"
        geometry = "geometry"
        partition_by = "state"

        [[layers]]
        name = "boundaries"
        resolution = "static"
        stac_item = "https://example.test/stac/tehsil.json"
        """
    )

    errors = validate_unique_entities([descriptor, descriptor], ["a.toml", "b.toml"])

    assert len(errors) == 1
    assert errors[0].field == "entity"


def test_descriptor_validation_matches_date_temporal_pattern() -> None:
    descriptor = EntityDescriptor.from_toml(
        """
        entity = "mws"
        key = "mws_id"
        geometry = "geometry"
        partition_by = "river_basin"

        [[layers]]
        name = "base"
        resolution = "static"
        stac_item = "https://example.test/stac/base.json"

        [layers.rename]
        uid = "mws_id"

        [[layers]]
        name = "deltaG_fortnight"
        resolution = "fortnightly"
        stac_item = "https://example.test/stac/deltag.json"
        temporal_pattern = "{col}_{DATE}"

        [layers.rename]
        uid = "mws_id"
        """
    )
    stac_items = {
        "https://example.test/stac/base.json": {
            "table:columns": [
                {"name": "uid"},
                {"name": "river_basin"},
                {"name": "geometry"},
            ]
        },
        "https://example.test/stac/deltag.json": {
            "table:columns": [
                {"name": "uid"},
                {"name": "deltaG_2021-01-01"},
            ]
        },
    }

    result = validate_descriptor(descriptor, stac_items)

    assert result.is_valid
