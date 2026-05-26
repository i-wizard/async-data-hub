from fastapi import APIRouter, Depends

from app.config.dependencies import webhook_service
from app.schemas.webhooks import TriggerWebhookRequest, TriggerWebhookResponse
from app.services.webhook_service import WebhookService

router = APIRouter()


@router.post(
    "/trigger-event",
    response_model=TriggerWebhookResponse,
    response_description="Fan out an event to multiple webhook targets",
)
async def trigger_event(
    data: TriggerWebhookRequest,
    _service: WebhookService = Depends(webhook_service),
) -> TriggerWebhookResponse:
    """
    Starts bounded webhook fan-out so the project can demonstrate retries,
    timeout handling, and persistence of distributed delivery attempts.
    """

    return await _service.trigger_event(data=data)
