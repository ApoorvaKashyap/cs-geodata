"""FastAPI router for GeoJSON layer conversion requests."""

from fastapi import APIRouter

from src.app.models import BaseLayers, ConversionRequest
from src.conversion.layers import base_layer_cache, handle_layers

router = APIRouter(prefix="/vector", tags=["vector"])


@router.post(path="/layers")
async def create_layer(request: ConversionRequest) -> dict[str, str]:
    """Handle layer conversion requests.

    Args:
        request: The API request payload.

    Returns:
        A dictionary containing the task ID and status.

    """
    return handle_layers(request)


@router.post(path="/create_base_cache")
async def create_base_cache(request: BaseLayers) -> dict[str, str]:
    """Handle base layer caching requests.

    Args:
        request: The API request payload.

    Returns:
        A dictionary containing the task ID and status.

    """
    return base_layer_cache(request)
