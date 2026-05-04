"""Redis-backed structured run progress."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field

from src.utils.configs import get_settings
from src.utils.redis_client import get_redis_client

RunStatus = Literal["pending", "running", "complete", "failed"]


class FetchProgress(BaseModel):
    """Represent fetch-level progress counts.

    Attributes:
        completed: Number of completed fetch units.
        total: Total fetch units expected.
    """

    completed: int = 0
    total: int = 0


class DescriptorProgress(BaseModel):
    """Represent progress for one Descriptor in a run.

    Attributes:
        entity: Entity name being processed.
        status: Current Descriptor status.
        phase: Current pipeline phase.
        layer: Current layer name.
        progress: Optional fetch progress counts.
        error: Failure message when status is ``failed``.
    """

    entity: str
    status: RunStatus = "pending"
    phase: str | None = None
    layer: str | None = None
    progress: FetchProgress | None = None
    error: str | None = None


class RunProgress(BaseModel):
    """Represent progress for a full pipeline run.

    Attributes:
        run_id: UUID string assigned to the run.
        status: Aggregate run status.
        descriptors: Descriptor progress entries.
    """

    run_id: str
    status: RunStatus
    descriptors: list[DescriptorProgress] = Field(default_factory=list)


class ProgressStore:
    """Persist and read run progress in Redis."""

    def __init__(self) -> None:
        """Initialize the progress store from configured Redis settings."""
        self.redis = get_redis_client()
        self.ttl_seconds = get_settings().redis_progress_ttl_seconds

    def initialise_run(self, run_id: UUID | str, entities: list[str]) -> RunProgress:
        """Create initial progress records for a run.

        Args:
            run_id: UUID assigned to the run.
            entities: Entity names included in the run.

        Returns:
            Initial run progress.
        """
        run_key = self._run_key(run_id)
        entity_names = list(dict.fromkeys(entities))
        self.redis.delete(run_key)

        for entity in entity_names:
            descriptor = DescriptorProgress(entity=entity)
            self.redis.rpush(run_key, entity)
            self._set_descriptor(run_id, descriptor)

        self.redis.expire(run_key, self.ttl_seconds)
        return self.get_run(run_id)

    def set_descriptor_status(
        self,
        run_id: UUID | str,
        entity: str,
        status: RunStatus,
        phase: str | None = None,
        layer: str | None = None,
        completed: int | None = None,
        total: int | None = None,
        error: str | None = None,
    ) -> DescriptorProgress:
        """Update progress for one Descriptor.

        Args:
            run_id: UUID assigned to the run.
            entity: Entity name being updated.
            status: New Descriptor status.
            phase: Current pipeline phase.
            layer: Current layer name.
            completed: Completed fetch count.
            total: Total fetch count.
            error: Failure message.

        Returns:
            Updated Descriptor progress.
        """
        progress = None
        if completed is not None or total is not None:
            progress = FetchProgress(completed=completed or 0, total=total or 0)

        descriptor = DescriptorProgress(
            entity=entity,
            status=status,
            phase=phase,
            layer=layer,
            progress=progress,
            error=error,
        )
        self._set_descriptor(run_id, descriptor)
        return descriptor

    def mark_descriptor_running(
        self,
        run_id: UUID | str,
        entity: str,
        phase: str,
        layer: str | None = None,
    ) -> DescriptorProgress:
        """Mark a Descriptor as running.

        Args:
            run_id: UUID assigned to the run.
            entity: Entity name being updated.
            phase: Current pipeline phase.
            layer: Current layer name.

        Returns:
            Updated Descriptor progress.
        """
        return self.set_descriptor_status(run_id, entity, "running", phase, layer)

    def mark_descriptor_complete(
        self,
        run_id: UUID | str,
        entity: str,
    ) -> DescriptorProgress:
        """Mark a Descriptor as complete.

        Args:
            run_id: UUID assigned to the run.
            entity: Entity name being updated.

        Returns:
            Updated Descriptor progress.
        """
        return self.set_descriptor_status(run_id, entity, "complete")

    def mark_descriptor_failed(
        self,
        run_id: UUID | str,
        entity: str,
        error: str,
    ) -> DescriptorProgress:
        """Mark a Descriptor as failed.

        Args:
            run_id: UUID assigned to the run.
            entity: Entity name being updated.
            error: Failure message.

        Returns:
            Updated Descriptor progress.
        """
        return self.set_descriptor_status(run_id, entity, "failed", error=error)

    def get_run(self, run_id: UUID | str) -> RunProgress:
        """Read aggregate progress for a run.

        Args:
            run_id: UUID assigned to the run.

        Returns:
            Aggregate run progress.
        """
        raw_entities = self.redis.lrange(self._run_key(run_id), 0, -1)
        entities = [
            entity.decode("utf-8") if isinstance(entity, bytes) else str(entity)
            for entity in raw_entities
        ]
        descriptors = [
            descriptor
            for entity in entities
            if (descriptor := self.get_descriptor(run_id, entity)) is not None
        ]

        return RunProgress(
            run_id=str(run_id),
            status=self._aggregate_status(descriptors),
            descriptors=descriptors,
        )

    def get_descriptor(
        self,
        run_id: UUID | str,
        entity: str,
    ) -> DescriptorProgress | None:
        """Read progress for one Descriptor.

        Args:
            run_id: UUID assigned to the run.
            entity: Entity name to read.

        Returns:
            Descriptor progress when present.
        """
        raw = self.redis.get(self._descriptor_key(run_id, entity))
        if raw is None:
            return None

        payload = raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)
        return DescriptorProgress.model_validate_json(payload)

    def _set_descriptor(
        self,
        run_id: UUID | str,
        descriptor: DescriptorProgress,
    ) -> None:
        self.redis.setex(
            self._descriptor_key(run_id, descriptor.entity),
            self.ttl_seconds,
            descriptor.model_dump_json(exclude_none=True),
        )
        self.redis.expire(self._run_key(run_id), self.ttl_seconds)

    def _aggregate_status(self, descriptors: list[DescriptorProgress]) -> RunStatus:
        if not descriptors:
            return "pending"
        statuses = {descriptor.status for descriptor in descriptors}
        if "failed" in statuses:
            return "failed"
        if statuses == {"complete"}:
            return "complete"
        if "running" in statuses:
            return "running"
        return "pending"

    def _run_key(self, run_id: UUID | str) -> str:
        return f"runs:{run_id}:entities"

    def _descriptor_key(self, run_id: UUID | str, entity: str) -> str:
        return f"runs:{run_id}:{entity}"
