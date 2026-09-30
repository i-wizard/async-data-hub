from typing import Any, Dict, List, Optional

from pydantic import BaseModel, HttpUrl


class TriggerWebhookRequest(BaseModel):
    """
    Captures webhook fan-out input so the API contract stays explicit and easy
    to validate before any outbound delivery work begins.
    """

    event_name: str
    payload: Dict[str, Any]
    targets: List[HttpUrl]


class WebhookAttemptResponse(BaseModel):
    """
    Returns one target attempt outcome so retries and partial failures are
    visible in the API response as well as in the database.
    """

    target_url: str
    attempt_number: int
    status: str
    response_status_code: Optional[int] = None
    error_message: Optional[str] = None


class TriggerWebhookResponse(BaseModel):
    """
    Summarizes a webhook fan-out so clients can see whether each target worked
    without reading delivery-attempt rows directly.
    """

    event_id: int
    status: str
    attempts: List[WebhookAttemptResponse]
