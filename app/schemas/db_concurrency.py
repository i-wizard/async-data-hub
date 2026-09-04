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
