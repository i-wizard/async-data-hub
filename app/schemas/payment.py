from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, model_validator


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
    error_message: Optional[str] = None
    idempotent_replayed: bool = False

    model_config = {"from_attributes": True}

    @model_validator(mode="before")
    def convert_customer_id_to_string(cls, values):
        """
        Ensure that customer_id is always returned as a string, even if it's stored as a UUID in the database.
        """
        values.customer_id = str(values.customer_id)
        return values