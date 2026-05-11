"""Pipeline orchestration jobs."""

from __future__ import annotations

import asyncio

import polars as pl

from src.descriptor.schema import EntityDescriptor, LayerResolution
from src.pipeline.fetch import fetch_layer
from src.pipeline.merge import merge_resolution_frames
from src.pipeline.normalise import normalise
from src.pipeline.reshape import reshape
from src.pipeline.write import write_resolution
from src.progress.store import ProgressStore
from src.stac.client import StacSourceInfo
from src.utils.workspace import run_workspace


def run_descriptor_job(
    run_id: str,
    descriptor_data: dict[str, object],
    output_root: str,
    stac_source_infos: dict[str, dict[str, object]],
) -> None:
    """Run one Descriptor pipeline job.

    Sequences Fetch → Normalise → Reshape → Write for every layer declared in
    the Descriptor. Creates a UUID-scoped scratch workspace that is cleaned up
    on both success and failure. Progress is written to Redis at every stage
    transition.

    Args:
        run_id: UUID string assigned to the parent run.
        descriptor_data: Serialized Descriptor model data.
        output_root: Root path or URI for ecolib output data.
        stac_source_infos: Mapping from ``stac_item`` URI to serialized
            :class:`~src.stac.client.StacSourceInfo` data. Avoids re-fetching
            STAC items inside the worker.

    Raises:
        Exception: Re-raises any pipeline failure after marking progress failed.
    """
    descriptor = EntityDescriptor.model_validate(descriptor_data)
    source_infos = {
        uri: _deserialize_source_info(uri, data)
        for uri, data in stac_source_infos.items()
    }
    progress = ProgressStore()

    try:
        progress.mark_descriptor_running(
            run_id,
            descriptor.entity,
            phase="orchestrate",
        )
        with run_workspace(run_id) as workspace:
            asyncio.run(
                _run_pipeline_stages(
                    descriptor,
                    source_infos,
                    output_root,
                    workspace_path=str(workspace),
                    run_id=run_id,
                    progress=progress,
                )
            )
        progress.mark_descriptor_complete(run_id, descriptor.entity)
    except Exception as exc:
        progress.mark_descriptor_failed(run_id, descriptor.entity, str(exc))
        raise


async def _run_pipeline_stages(
    descriptor: EntityDescriptor,
    source_infos: dict[str, StacSourceInfo],
    output_root: str,
    workspace_path: str,
    run_id: str,
    progress: ProgressStore,
) -> None:
    from pathlib import Path

    scratch = Path(workspace_path)
    outputs: dict[LayerResolution, list[tuple[pl.LazyFrame, StacSourceInfo]]] = {}

    for layer in descriptor.layers:
        source_info = source_infos.get(layer.stac_item)
        if source_info is None:
            raise RuntimeError(
                f"No STAC source info for layer '{layer.name}' "
                f"(stac_item='{layer.stac_item}')."
            )

        layer_scratch = scratch / layer.name
        layer_scratch.mkdir(parents=True, exist_ok=True)

        progress.mark_descriptor_running(
            run_id,
            descriptor.entity,
            phase="fetch",
            layer=layer.name,
        )
        frame = await fetch_layer(layer, source_info, layer_scratch)

        progress.mark_descriptor_running(
            run_id,
            descriptor.entity,
            phase="normalise",
            layer=layer.name,
        )
        frame = normalise(frame, layer, source_info.column_types)

        progress.mark_descriptor_running(
            run_id,
            descriptor.entity,
            phase="reshape",
            layer=layer.name,
        )
        frame = reshape(frame, layer, descriptor.key)
        outputs.setdefault(layer.resolution, []).append((frame, source_info))

    for resolution, items in outputs.items():
        progress.mark_descriptor_running(
            run_id,
            descriptor.entity,
            phase="merge",
        )
        merged = merge_resolution_frames(
            [frame for frame, _ in items],
            resolution,
            descriptor.key,
        )

        progress.mark_descriptor_running(
            run_id,
            descriptor.entity,
            phase="write",
        )
        write_resolution(
            merged,
            resolution,
            descriptor,
            items[0][1],
            output_root,
        )


def _deserialize_source_info(
    uri: str,
    data: dict[str, object],
) -> StacSourceInfo:
    return StacSourceInfo(
        stac_item_uri=uri,
        columns=set(data.get("columns", [])),  # type: ignore[arg-type]
        column_types=data.get("column_types", {}),  # type: ignore[arg-type]
        crs=data.get("crs"),  # type: ignore[arg-type]
        asset_href=data["asset_href"],  # type: ignore[arg-type]
        source_type=data["source_type"],  # type: ignore[arg-type]
    )
