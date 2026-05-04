"""Run workspace helpers."""

from __future__ import annotations

import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from uuid import UUID

from src.utils.configs import get_settings


def get_run_workspace(run_id: UUID | str) -> Path:
    """Return the filesystem workspace path for a run.

    Args:
        run_id: UUID assigned to the pipeline run.

    Returns:
        Absolute path below the configured run workspace root.
    """
    return get_settings().run_workspace_root / str(run_id)


def create_run_workspace(run_id: UUID | str) -> Path:
    """Create and return the workspace directory for a run.

    Args:
        run_id: UUID assigned to the pipeline run.

    Returns:
        Created workspace path.
    """
    workspace = get_run_workspace(run_id)
    workspace.mkdir(parents=True, exist_ok=False)
    return workspace


def cleanup_run_workspace(run_id: UUID | str) -> None:
    """Remove a run workspace if it exists.

    Args:
        run_id: UUID assigned to the pipeline run.
    """
    shutil.rmtree(get_run_workspace(run_id), ignore_errors=True)


@contextmanager
def run_workspace(run_id: UUID | str) -> Iterator[Path]:
    """Create a run workspace and clean it up after use.

    Args:
        run_id: UUID assigned to the pipeline run.

    Yields:
        Created workspace path.
    """
    workspace = create_run_workspace(run_id)
    try:
        yield workspace
    finally:
        cleanup_run_workspace(run_id)
