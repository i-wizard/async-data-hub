from typing import Dict

from fastapi import APIRouter, Response, status

router = APIRouter(prefix="/mock-webhooks")


@router.post(
    "/success",
    response_model=Dict,
    response_description="Mock webhook target that succeeds",
)
async def successful_webhook() -> Dict:
    """
    Provides a local success target so webhook fan-out can be exercised without
    relying on external callback infrastructure.
    """

    return {"status": "accepted"}


@router.post(
    "/failure",
    response_model=Dict,
    response_description="Mock webhook target that fails",
)
async def failing_webhook(response: Response) -> Dict:
    """
    Provides a local failure target so retry and partial-delivery behavior can
    be tested and demonstrated deterministically.
    """

    response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "unavailable"}
