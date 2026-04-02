from loguru import logger

from src.app.models import LayerConversionRequest
from src.conversion.algos.mws import run_mws_pipeline
from src.work.work_queue import lq


def handle_layers(request: LayerConversionRequest) -> dict:
    logger.info(f"Enqueueing layer conversion job for unit={request.unit}")
    tid = lq.enqueue(layer_conversion, request)
    return {
        "task_id": tid.id,
        "status": tid.get_status().name,
    }


def layer_conversion(request: LayerConversionRequest) -> None:
    logger.info(
        f"Starting layer conversion: unit={request.unit}, layers={list(request.layers.keys())}"
    )
    try:
        output = run_mws_pipeline(request)
        logger.info(f"Layer conversion complete -> {output}")
    except Exception as e:
        logger.error(f"Layer conversion failed: {e}")
        raise
