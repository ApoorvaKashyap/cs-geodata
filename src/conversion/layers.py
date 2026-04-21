from loguru import logger
from rq.job import Job

from src.app.models import BaseLayers, LayerConversionRequest
from src.conversion.algos import run_mws_pipeline
from src.conversion.helpers.api import convert_base
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
        f"Starting layer conversion: unit={request.unit}, layers={list(request.layers)}"
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
