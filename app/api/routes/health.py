from fastapi import APIRouter, Depends

from app.config.dependencies import health_service
from app.schemas.health import HealthStatusResponse
from app.services.health_service import HealthService

router = APIRouter()


@router.get(
    "/health",
    response_model=HealthStatusResponse,
    response_description="Report API dependency health",
)
async def get_health(
    _service: HealthService = Depends(health_service),
) -> HealthStatusResponse:
    """
    Exposes a lightweight readiness endpoint because async systems need a fast
    way to verify shared infrastructure before real traffic is sent.
    """

    return await _service.get_status()
