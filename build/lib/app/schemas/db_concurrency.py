from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class IsolationLevel(str, Enum):
    """The transaction isolation level to run a transfer under.

        READ_COMMITTED  - Postgres default; sees each statement's own snapshot.
        REPEATABLE_READ - snapshot fixed at first statement; blocks lost updates.
        SERIALIZABLE    - as if transactions ran one-at-a-time; blocks write skew.
    """
    READ_COMMITTED = "read_committed"
    REPEATABLE_READ = "repeatable_read"
    SERIALIZABLE = "serializable"



class CreateAccountRequest(BaseModel):
    id: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    balance: int = Field(ge=0)


class AccountResponse(BaseModel):
    id: str
    owner: str
    balance: int

    model_config = {"from_attributes": True}

class TransferRequest(BaseModel):
    """Move `amount` (cents) from one account to another."""
    from_id: str
    to_id: str
    amount:  int = Field(gt=0)


class TransferResponse(BaseModel):
    """Outcome of a completed transfer."""
    from_id: str
    to_id: str
    amount: int
    from_balance: int
    to_balance: int
    isolation_level: IsolationLevel
    # How many times the transfer was retried due to serialization failures.
    retries: int


class EnqueuedJobStatus(str, Enum):
    """Lifecycle of a job in the queue."""

    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"



class ClaimStrategy(str, Enum):
    """How a worker locks the next queued row.

    SKIP_LOCKED - FOR UPDATE SKIP LOCKED: ignore rows other workers hold; the
                  correct pattern for a queue (no blocking, no double-claim).
    BLOCKING    - FOR UPDATE: wait for a locked row (workers serialize — bad for
                  a queue; shown for contrast).
    NOWAIT      - FOR UPDATE NOWAIT: fail immediately (55P03) if the row is locked.
    """

    SKIP_LOCKED = "skip_locked"
    BLOCKING = "blocking"
    NOWAIT = "nowait"


class EnqueueRequest(BaseModel):
    """Add a job to the queue."""

    payload: str = Field(min_length=1)


class EnqueuedJobResponse(BaseModel):
    """A job's current state."""

    id: str
    position: int
    payload: str
    status: EnqueuedJobStatus
    locked_by: str | None = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ClaimResponse(BaseModel):
    """Result of a claim attempt: the claimed job, or nothing available."""

    claimed: bool
    job: EnqueuedJobResponse | None = None