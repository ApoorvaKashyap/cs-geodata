from fastapi import APIRouter

from src.app.models import LayerConversionRequest
from src.conversion.layers import handle_layers

router = APIRouter(prefix="/vector", tags=["vector"])


@router.post(path="/layers")
async def create_layer(request: LayerConversionRequest) -> dict[str, str]:
    return handle_layers(request)
