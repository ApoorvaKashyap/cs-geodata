"""FastAPI routes for Descriptor validation and pipeline runs."""

from __future__ import annotations

import tomllib
from typing import TextIO, cast

import fsspec
from fastapi import APIRouter, HTTPException
from pydantic import ValidationError
from redis.exceptions import RedisError

from src.api.models import (
    DescriptorValidateRequest,
    DescriptorValidateResponse,
)
from src.descriptor.schema import EntityDescriptor
from src.descriptor.validator import (
    DescriptorValidationError,
    validate_descriptor,
)
from src.progress.store import ProgressStore, RunProgress
from src.stac.client import StacClient, StacClientError, StacItem

router = APIRouter(tags=["pipeline"])


@router.get("/runs/{run_id}", response_model=RunProgress)
async def get_run_status(run_id: str) -> RunProgress:
    """Return structured progress for a pipeline run.

    Args:
        run_id: UUID assigned to the run.

    Returns:
        Structured run progress from Redis.

    Raises:
        HTTPException: If the run is unknown or Redis cannot be read.
    """
    store = ProgressStore()
    try:
        if not store.has_run(run_id):
            raise HTTPException(status_code=404, detail="Run not found.")
        return store.get_run(run_id)
    except RedisError as exc:
        raise HTTPException(
            status_code=503, detail=f"Redis unavailable: {exc}"
        ) from exc


@router.post("/descriptors/validate", response_model=DescriptorValidateResponse)
async def validate_descriptor_endpoint(
    request: DescriptorValidateRequest,
) -> DescriptorValidateResponse:
    """Validate a Descriptor against live STAC item metadata.

    Args:
        request: Descriptor validation request.

    Returns:
        Validation response with the parsed Descriptor and structured errors.
    """
    descriptor, parse_errors = _read_descriptor(request.descriptor_uri)
    if descriptor is None:
        return DescriptorValidateResponse(
            descriptor_uri=request.descriptor_uri,
            valid=False,
            errors=parse_errors,
        )

    stac_items, stac_errors = await _fetch_stac_items(
        descriptor, request.descriptor_uri
    )
    result = validate_descriptor(
        descriptor=descriptor,
        stac_items=stac_items,
        descriptor_uri=request.descriptor_uri,
    )
    errors = [*stac_errors, *result.errors]

    return DescriptorValidateResponse(
        descriptor_uri=request.descriptor_uri,
        valid=not errors,
        descriptor=descriptor,
        errors=errors,
    )


def _read_descriptor(
    descriptor_uri: str,
) -> tuple[EntityDescriptor | None, list[DescriptorValidationError]]:
    try:
        with fsspec.open(descriptor_uri, mode="rt", encoding="utf-8") as file:
            content = cast(TextIO, file).read()
        return EntityDescriptor.from_toml(content), []
    except FileNotFoundError:
        return None, [_descriptor_error(descriptor_uri, "descriptor_uri", "Not found.")]
    except tomllib.TOMLDecodeError as exc:
        return None, [_descriptor_error(descriptor_uri, "descriptor", str(exc))]
    except ValidationError as exc:
        return None, _pydantic_errors(descriptor_uri, exc)
    except OSError as exc:
        return None, [_descriptor_error(descriptor_uri, "descriptor_uri", str(exc))]
    except Exception as exc:
        return None, [_descriptor_error(descriptor_uri, "descriptor_uri", str(exc))]


async def _fetch_stac_items(
    descriptor: EntityDescriptor,
    descriptor_uri: str,
) -> tuple[dict[str, StacItem], list[DescriptorValidationError]]:
    client = StacClient()
    items: dict[str, StacItem] = {}
    errors: list[DescriptorValidationError] = []

    for layer in descriptor.layers:
        if layer.stac_item in items:
            continue

        try:
            items[layer.stac_item] = await client.fetch_item(layer.stac_item)
        except StacClientError as exc:
            errors.append(
                DescriptorValidationError(
                    descriptor_uri=descriptor_uri,
                    layer=layer.name,
                    field="stac_item",
                    message=str(exc),
                )
            )

    return items, errors


def _pydantic_errors(
    descriptor_uri: str,
    exc: ValidationError,
) -> list[DescriptorValidationError]:
    errors: list[DescriptorValidationError] = []
    for error in exc.errors():
        location = ".".join(str(part) for part in error["loc"])
        errors.append(
            _descriptor_error(
                descriptor_uri=descriptor_uri,
                field=location or "descriptor",
                message=str(error["msg"]),
            )
        )
    return errors


def _descriptor_error(
    descriptor_uri: str,
    field: str,
    message: str,
) -> DescriptorValidationError:
    return DescriptorValidationError(
        descriptor_uri=descriptor_uri,
        field=field,
        message=message,
    )
