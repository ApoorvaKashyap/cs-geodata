from fastapi import APIRouter

from src.app.models import BaseLayers, LayerConversionRequest
from src.conversion.layers import base_layer_cache, handle_layers

router = APIRouter(prefix="/vector", tags=["vector"])


@router.post(path="/layers")
async def create_layer(request: LayerConversionRequest) -> dict[str, str]:
    return handle_layers(request)


@router.post(path="/create_base_cache")
async def create_base_cache(request: BaseLayers) -> dict[str, str]:
    return base_layer_cache(request)
