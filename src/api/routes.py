"""FastAPI routes for Descriptor validation and pipeline runs."""

from __future__ import annotations

import tomllib
from typing import TextIO, cast
from uuid import uuid4

import fsspec
from fastapi import APIRouter, HTTPException
from pydantic import ValidationError
from redis.exceptions import RedisError

from src.api.models import (
    DescriptorValidateRequest,
    DescriptorValidateResponse,
    RunRequest,
    RunResponse,
)
from src.descriptor.schema import EntityDescriptor
from src.descriptor.validator import (
    DescriptorValidationError,
    validate_descriptor,
    validate_unique_entities,
)
from src.pipeline.orchestrator import run_descriptor_job
from src.progress.store import ProgressStore, RunProgress
from src.stac.client import StacClient, StacClientError, StacItem, StacSourceInfo
from src.utils.configs import get_settings
from src.work.work_queue import get_runs_queue

router = APIRouter(tags=["pipeline"])


@router.post("/runs", response_model=RunResponse)
async def create_run(request: RunRequest) -> RunResponse:
    """Validate Descriptors and enqueue a pipeline run.

    Args:
        request: Run creation request.

    Returns:
        Run ID for progress polling.

    Raises:
        HTTPException: If validation fails or Redis/RQ is unavailable.
    """
    descriptors, stac_info_map, errors = await _read_and_validate_descriptors(
        request.descriptors
    )
    errors.extend(validate_unique_entities(descriptors, request.descriptors))
    if errors:
        raise HTTPException(
            status_code=422,
            detail=[error.model_dump(exclude_none=True) for error in errors],
        )

    run_id = str(uuid4())
    settings = get_settings()

    try:
        ProgressStore().initialise_run(
            run_id,
            [descriptor.entity for descriptor in descriptors],
        )
        queue = get_runs_queue()
        for descriptor in descriptors:
            serialized_infos = {
                uri: _serialize_source_info(info)
                for uri, info in stac_info_map.items()
                if any(layer.stac_item == uri for layer in descriptor.layers)
            }
            queue.enqueue(
                run_descriptor_job,
                run_id,
                descriptor.model_dump(mode="json"),
                request.output_root,
                serialized_infos,
                job_timeout=settings.rq_job_timeout,
            )
    except RedisError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Redis or RQ unavailable: {exc}",
        ) from exc

    return RunResponse(run_id=run_id)


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


async def _read_and_validate_descriptors(
    descriptor_uris: list[str],
) -> tuple[
    list[EntityDescriptor], dict[str, StacSourceInfo], list[DescriptorValidationError]
]:
    descriptors: list[EntityDescriptor] = []
    all_source_infos: dict[str, StacSourceInfo] = {}
    errors: list[DescriptorValidationError] = []

    for descriptor_uri in descriptor_uris:
        descriptor, parse_errors = _read_descriptor(descriptor_uri)
        if descriptor is None:
            errors.extend(parse_errors)
            continue

        stac_items, source_infos, stac_errors = await _fetch_stac_items(
            descriptor, descriptor_uri
        )
        validation_result = validate_descriptor(
            descriptor=descriptor,
            stac_items=stac_items,
            descriptor_uri=descriptor_uri,
        )
        descriptors.append(descriptor)
        all_source_infos.update(source_infos)
        errors.extend(stac_errors)
        errors.extend(validation_result.errors)

    return descriptors, all_source_infos, errors


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
) -> tuple[
    dict[str, StacItem], dict[str, StacSourceInfo], list[DescriptorValidationError]
]:
    client = StacClient()
    items: dict[str, StacItem] = {}
    source_infos: dict[str, StacSourceInfo] = {}
    errors: list[DescriptorValidationError] = []

    for layer in descriptor.layers:
        if layer.stac_item in items:
            continue

        try:
            item = await client.fetch_item(layer.stac_item)
            items[layer.stac_item] = item
            source_infos[layer.stac_item] = client.resolve_source_info(
                layer.stac_item, item
            )
        except StacClientError as exc:
            errors.append(
                DescriptorValidationError(
                    descriptor_uri=descriptor_uri,
                    layer=layer.name,
                    field="stac_item",
                    message=str(exc),
                )
            )

    return items, source_infos, errors


def _serialize_source_info(info: StacSourceInfo) -> dict[str, object]:
    return {
        "columns": list(info.columns),
        "column_types": info.column_types,
        "crs": info.crs,
        "asset_href": info.asset_href,
        "source_type": info.source_type,
    }


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
