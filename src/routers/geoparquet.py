"""FastAPI router for GeoParquet standardisation requests."""

from fastapi import APIRouter

from src.app.models import StandardiseRequest
from src.conversion.layers import handle_standardise

router = APIRouter(prefix="/vector", tags=["vector"])


@router.post(path="/standardise")
async def create_standardise(request: StandardiseRequest) -> dict[str, str]:
    """Handle standardise requests.

    Args:
        request: The API request payload.

    Returns:
        A dictionary containing the task ID and status.

    """
    return handle_standardise(request)
