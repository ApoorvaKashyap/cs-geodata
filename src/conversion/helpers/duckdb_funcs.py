"""DuckDB connection and extension initialization utilities."""

import tempfile
import uuid

import duckdb
from duckdb import DuckDBPyConnection
from loguru import logger

from src.utils.configs import settings


def init_duckdb() -> DuckDBPyConnection:
    """Initialize a DuckDB connection with required extensions and settings.

    Sets up a temporary DuckDB database file, loads spatial and S3 extensions,
    and configures AWS credentials using the default credential chain.

    Returns:
        An active DuckDB connection object.

    Raises:
        RuntimeError: If DuckDB initialization fails.

    """
    extensions = ["httpfs", "spatial", "aws"]
    try:
        import os
        import weakref

        tmp_dir = tempfile.TemporaryDirectory(prefix="duckdb_db_")
        tmp_db_path = os.path.join(tmp_dir.name, "duckdb.db")
        conn = duckdb.connect(tmp_db_path)
        weakref.finalize(conn, tmp_dir.cleanup)
        for ext in extensions:
            conn.install_extension(ext)
            conn.load_extension(ext)
            logger.info(f"Installed DuckDB extensions: {ext}")
        logger.debug(
            f"S3 Creds: {settings.aws_access_key_id}, "
            f"{settings.aws_secret_access_key}, {settings.aws_region}"
        )
        conn.execute("""CREATE OR REPLACE SECRET secret (
                TYPE s3,
                PROVIDER credential_chain
            );""")
        conn.execute(f"SET memory_limit = '{settings.duckdb_memory_limit}'")
        conn.execute(f"SET threads = {settings.duckdb_threads}")
        unique_suffix = uuid.uuid4().hex[:8]
        conn.execute(
            f"SET temp_directory = '{settings.duckdb_temp_dir}_{unique_suffix}'"
        )
        conn.execute("SET preserve_insertion_order = false")

        return conn
    except Exception as e:
        raise RuntimeError(f"Failed to initialize DuckDB: {e}") from e


def get_duckdb_geom_expr(
    conn: DuckDBPyConnection, table_or_view: str = "merged"
) -> str:
    """Return SQL expression to access geometry as a DuckDB GEOMETRY.

    If the geometry column is already typed as GEOMETRY (e.g. from GeoParquet),
    returns 'geometry'. If it is a binary WKB blob, returns 'ST_GeomFromWKB(geometry)'.

    Args:
        conn: Active DuckDB connection.
        table_or_view (str): Table or view name to inspect. Defaults to "merged".

    Returns:
        SQL expression string to reference geometry as a GEOMETRY object.

    """
    row = conn.execute(
        f"SELECT column_type FROM (DESCRIBE {table_or_view}) WHERE lower(column_name) = 'geometry'"
    ).fetchone()
    if row and row[0].upper().startswith("GEOMETRY"):
        return "geometry"
    return "ST_GeomFromWKB(geometry)"
