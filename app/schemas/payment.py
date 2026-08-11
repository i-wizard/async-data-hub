from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field



class ChargeStatus(str, Enum):
    """Lifecycle state of a charge."""

    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"

class PaymentRequestProcessingStatus(str, Enum):
    processing = "PROCESSING"
    completed = "COMPLETED"



class CreatePaymentRequest(BaseModel):
    amount: int = Field(gt=0, description="Amount in minor units, e.g. cents.")
    customer_id: str = Field(min_length=1)


class PaymentResponse(BaseModel):
    id: str
    amount: int
    customer_id: str
    status: ChargeStatus
    created_at: datetime
    idempotency_key: str
    idempotent_replayed: bool = False

    model_config = {"from_attributes": True}