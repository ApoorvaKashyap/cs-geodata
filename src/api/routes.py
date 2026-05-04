"""FastAPI routes for Descriptor validation and pipeline runs."""

from __future__ import annotations

import asyncio
import json
import tomllib
from collections.abc import AsyncIterator
from typing import TextIO, cast

import fsspec
from fastapi import APIRouter
from pydantic import ValidationError
from redis.exceptions import RedisError
from rq.exceptions import NoSuchJobError
from rq.job import Job
from starlette.responses import StreamingResponse

from src.api.models import (
    DescriptorValidateRequest,
    DescriptorValidateResponse,
)
from src.descriptor.schema import EntityDescriptor
from src.descriptor.validator import (
    DescriptorValidationError,
    validate_descriptor,
)
from src.stac.client import StacClient, StacClientError, StacItem
from src.utils.redis_client import get_redis_client

router = APIRouter(tags=["pipeline"])
TASK_TERMINAL_STATUSES = {"finished", "failed", "stopped", "canceled"}


@router.get("/tasks/{task_id}")
async def stream_task_status(task_id: str) -> StreamingResponse:
    """Stream RQ task status changes as server-sent events.

    Args:
        task_id: RQ job ID to watch.

    Returns:
        SSE response that emits an event whenever the job status changes.
    """
    return StreamingResponse(
        _task_status_events(task_id),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def _task_status_events(task_id: str) -> AsyncIterator[str]:
    last_status: str | None = None

    while True:
        status = _get_task_status(task_id)
        if status != last_status:
            yield _sse_event("status", {"task_id": task_id, "status": status})
            last_status = status

        if status in TASK_TERMINAL_STATUSES or status.startswith("redis_error"):
            return

        await asyncio.sleep(1)


def _get_task_status(task_id: str) -> str:
    try:
        job = Job.fetch(task_id, connection=get_redis_client())
    except NoSuchJobError:
        return "not_found"
    except RedisError as exc:
        return f"redis_error: {exc}"
    return job.get_status().value


def _sse_event(event: str, payload: dict[str, str]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload)}\n\n"


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
