import uuid
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class ReserveStrategy(str, Enum):
    """The concurrency-control strategy used to perform a reservation.

    Each maps to a distinct technique so the demos can contrast them:
      NAIVE       - read-modify-write with no guard (the bug; oversells)
      OPTIMISTIC  - version-column compare-and-swap + bounded retry
      PESSIMISTIC - SELECT ... FOR UPDATE (row lock)
      ATOMIC      - single UPDATE ... WHERE stock >= qty (DB-level CAS)
      LOCKED      - Redis distributed lock around the critical section
    """

    NAIVE = "naive"
    OPTIMISTIC = "optimistic"
    PESSIMISTIC = "pessimistic"
    ATOMIC = "atomic"
    LOCKED = "locked"

class ReserveRequest(BaseModel):
    quantity: int = Field(..., gt=0)

class ProductRequest(BaseModel):
    name: str
    stock: int = Field(..., gt=0)


class ProductResponse(BaseModel):
    id: uuid.UUID
    name: str
    stock: int

    model_config = {"from_attributes": True}


class ProductReservationResponse(BaseModel):
    id: uuid.UUID
    product_id: uuid.UUID
    quantity: int
    created_at: datetime

    model_config = {"from_attributes": True}


