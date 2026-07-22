from fastapi import APIRouter

from src.app.models import StandardiseRequest
from src.conversion.layers import handle_standardise

router = APIRouter(prefix="/vector", tags=["vector"])


@router.post(path="/standardise")
async def create_standardise(request: StandardiseRequest) -> dict[str, str]:
    return handle_standardise(request)
