"""Abstract base class for pipeline data sources."""

from __future__ import annotations

from abc import ABC, abstractmethod

import polars as pl


class AbstractSource(ABC):
    """Base class for all pipeline data acquisition sources.

    Each concrete source fetches raw data from one origin and emits a
    single Polars LazyFrame with source-column names unchanged.
    """

    @abstractmethod
    async def fetch(self) -> pl.LazyFrame:
        """Fetch raw source data and return it as a LazyFrame.

        Returns:
            LazyFrame with raw source columns.

        Raises:
            RuntimeError: If the source cannot be fetched.
        """
