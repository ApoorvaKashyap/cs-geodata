from loguru import logger

from src.app.models import BaseLayers, ConversionRequest, load_descriptor
from src.conversion.algos import run_mws_pipeline
from src.conversion.helpers.api import convert_base
from src.work.work_queue import bq, lq


def handle_layers(request: ConversionRequest) -> dict:
    logger.info(f"Enqueueing conversion job for descriptor={request.descriptor_url}")
    tid = lq.enqueue(layer_conversion, request, job_timeout=86400)
    return {
        "task_id": tid.id,
        "status": tid.get_status().name,
    }


def layer_conversion(request: ConversionRequest) -> None:
    """RQ worker entry point.

    Fetches the TOML descriptor fresh from the URL, builds a
    LayerConversionRequest, and runs the pipeline.

    Args:
        request: Lightweight API payload containing the descriptor URL and
            output path.
    """
    logger.info(f"Loading descriptor from {request.descriptor_url}")
    full_request = load_descriptor(request.descriptor_url, request.output_path)
    logger.info(
        f"Starting layer conversion: entity={full_request.entity}, "
        f"layers={[d.name for d in full_request.attribute_layers]}"
    )
    try:
        import asyncio

        asyncio.run(run_mws_pipeline(full_request))
        logger.info(f"Layer conversion complete -> {request.output_path}")
    except Exception as e:
        logger.error(f"Layer conversion failed: {e}")
        raise


def base_layer_cache(request: BaseLayers) -> dict[str, str]:
    logger.info(f"Starting base layer cache for {request}")
    from rq.job import Job

    try:
        bid: Job = bq.enqueue(
            convert_base,
            request.base_layer_source,
            request.output_path,
            chunk_size=500000,
            super_layer_source=request.super_layer_source,
            super_field=request.super_field,
            job_timeout=86400,
        )
        return {
            "task_id": bid.id,
            "status": bid.get_status().name,
        }
    except Exception as e:
        logger.error(f"Base layer cache failed: {e}")
        return {}
