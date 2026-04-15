import random

import duckdb
from duckdb import DuckDBPyConnection
from loguru import logger

from src.utils.configs import settings


def init_duckdb() -> DuckDBPyConnection:
    extensions = ["httpfs", "spatial", "aws"]
    try:
        conn = duckdb.connect(f"/tmp/duckdb_{random.randint(0, 10000)}.db")
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

        return conn
    except Exception as e:
        raise RuntimeError(f"Failed to initialize DuckDB: {e}") from e
