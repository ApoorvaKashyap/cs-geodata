"""DuckDB connection helpers."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import duckdb
from duckdb import DuckDBPyConnection

from src.utils.configs import get_settings


def create_duckdb_connection(database: str = ":memory:") -> DuckDBPyConnection:
    """Create a DuckDB connection configured for pipeline writes.

    Args:
        database: DuckDB database path. Defaults to an in-memory database.

    Returns:
        Configured DuckDB connection with the spatial extension loaded.
    """
    settings = get_settings()
    connection = duckdb.connect(database=database)
    connection.execute(f"SET memory_limit = '{settings.duckdb_memory_limit}'")
    connection.execute(f"SET threads = {settings.duckdb_threads}")
    connection.execute("INSTALL spatial")
    connection.execute("LOAD spatial")
    return connection


@contextmanager
def duckdb_connection(database: str = ":memory:") -> Iterator[DuckDBPyConnection]:
    """Yield a configured DuckDB connection and close it afterward.

    Args:
        database: DuckDB database path. Defaults to an in-memory database.

    Yields:
        Configured DuckDB connection.
    """
    connection = create_duckdb_connection(database)
    try:
        yield connection
    finally:
        connection.close()
