"""Pipeline orchestration jobs."""

from __future__ import annotations

from src.descriptor.schema import EntityDescriptor
from src.progress.store import ProgressStore


def run_descriptor_job(
    run_id: str,
    descriptor_data: dict[str, object],
    output_root: str,
) -> None:
    """Run one Descriptor pipeline job.

    Args:
        run_id: UUID string assigned to the parent run.
        descriptor_data: Serialized Descriptor model data.
        output_root: Root path or URI for ecolib output data.

    Raises:
        Exception: Re-raises any pipeline failure after marking progress failed.
    """
    descriptor = EntityDescriptor.model_validate(descriptor_data)
    progress = ProgressStore()

    try:
        progress.mark_descriptor_running(
            run_id,
            descriptor.entity,
            phase="orchestrate",
        )
        _run_pipeline_stages(descriptor, output_root)
        progress.mark_descriptor_complete(run_id, descriptor.entity)
    except Exception as exc:
        progress.mark_descriptor_failed(run_id, descriptor.entity, str(exc))
        raise


def _run_pipeline_stages(
    descriptor: EntityDescriptor,
    output_root: str,
) -> None:
    """Run pipeline stages for one Descriptor.

    Args:
        descriptor: Descriptor to process.
        output_root: Root path or URI for ecolib output data.
    """
    _ = descriptor
    _ = output_root
