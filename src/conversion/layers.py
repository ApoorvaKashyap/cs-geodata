import os

from boto3 import client
from loguru import logger
from rq.job import Job

from src.app.models import BaseLayers, LayerConversionRequest
from src.conversion.algos.mws import run_mws_pipeline
from src.conversion.helpers.duckdb_funcs import init_duckdb
from src.utils.configs import settings
from src.work.work_queue import bq, lq


def handle_layers(request: LayerConversionRequest) -> dict:
    logger.info(f"Enqueueing layer conversion job for unit={request.unit}")
    tid = lq.enqueue(layer_conversion, request)
    return {
        "task_id": tid.id,
        "status": tid.get_status().name,
    }


def layer_conversion(request: LayerConversionRequest) -> None:
    logger.info(
        f"Starting layer conversion: unit={request.unit}, "
        f"layers={list(request.layers.keys())}"
    )
    try:
        output = run_mws_pipeline(request)
        logger.info(f"Layer conversion complete -> {output}")
    except Exception as e:
        logger.error(f"Layer conversion failed: {e}")
        raise


def base_layer_cache(request: BaseLayers) -> dict[str, str]:
    logger.info(f"Starting base layer cache for {request}")
    bid: Job = Job()
    try:
        for i in request.layers:
            match i:
                case "mws":
                    bid = bq.enqueue(convert_base, "mws", request.layers[i])
                case _:
                    raise ValueError(f"Unknown base layer request: {request}")
        return {
            "task_id": bid.id,
            "status": bid.get_status().name,
        }
    except Exception as e:
        logger.error(f"Base layer cache failed: {e}")
        return {}


def convert_base(layer: str, filepath: str) -> None:
    conn = init_duckdb()
    try:
        s3 = client("s3", region_name=settings.aws_region)
        with conn:
            os.makedirs("/tmp/layers", exist_ok=True)
            conn.sql(
                f"COPY (SELECT * FROM '{filepath}' ORDER BY ST_HILBERT(geom)) TO "
                f"'/tmp/layers/{layer}.parquet' "
                f"(FORMAT PARQUET, COMPRESSION 'ZSTD', COMPRESSION_LEVEL 15);"
            )
            s3.upload_file(
                f"/tmp/layers/{layer}.parquet", f"{settings.S3_BASE + layer}.parquet"
            )
        conn.close()
    except Exception as e:
        conn.close()
        logger.error(f"Failed to convert base layer: {e}")
        raise
