"""
PURE isolation-level knowledge — no framework, no I/O, standard library only.

Two things live here:
  1. Detecting a serialization failure (SQLSTATE 40001) so the caller knows to
     RETRY the whole transaction.
  2. The anomaly matrix for POSTGRES: which read phenomena each isolation level
     still permits. Note Postgres is stricter than the SQL standard —
     it never allows dirty reads, and REPEATABLE READ (snapshot isolation) also
     prevents phantom reads. The one anomaly REPEATABLE READ still allows is
     write skew, which only SERIALIZABLE prevents.
"""

import asyncio
from typing import Dict, Tuple

from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Account
from app.schemas.db_concurrency import AccountResponse, IsolationLevel, TransferResponse

# Postgres raises this SQLSTATE when it must abort a transaction to preserve
# isolation (a lost-update conflict under RR, or a dangerous cycle under SSI).
SQLSTATE_SERIALIZATION_FAILURE = "40001"

# level -> {phenomenon -> is it still possible?} for PostgreSQL.
_ANOMALY_MATRIX: Dict[str, Dict[str, bool]] = {
    "read_committed": {
        "dirty_read": False,
        "non_repeatable_read": True,
        "phantom_read": True,
        "lost_update": True,
        "write_skew": True,
    },
    "repeatable_read": {
        "dirty_read": False,
        "non_repeatable_read": False,  # snapshot fixed for the whole txn
        "phantom_read": False,  # snapshot isolation also stops phantoms
        "lost_update": False,  # detected -> aborts with 40001
        "write_skew": True,  # the anomaly RR still permits
    },
    "serializable": {
        "dirty_read": False,
        "non_repeatable_read": False,
        "phantom_read": False,
        "lost_update": False,
        "write_skew": False,  # SSI detects the cycle -> aborts with 40001
    },
}


def is_serialization_failure(sqlstate: str) -> bool:
    """Return True if the SQLSTATE is a serialization failure (retry the txn)."""
    return sqlstate == SQLSTATE_SERIALIZATION_FAILURE


def anomaly_possible(level: str, phenomenon: str) -> bool:
    """Return True if `phenomenon` can still occur at isolation `level` in Postgres."""
    return _ANOMALY_MATRIX[level][phenomenon]


_ISOLATION_SQL = {
    IsolationLevel.READ_COMMITTED: "READ COMMITTED",
    IsolationLevel.REPEATABLE_READ: "REPEATABLE READ",
    IsolationLevel.SERIALIZABLE: "SERIALIZABLE",
}

_READ_BALANCE = text("SELECT balance FROM accounts WHERE id = :id")
_DEBIT = text("UPDATE accounts SET balance = balance - :amt WHERE id = :id")
_CREDIT = text("UPDATE accounts SET balance = balance + :amt WHERE id = :id")


class InsufficientFunds(Exception):
    """Raised when the source account cannot cover the transfer (-> 409)."""


class SerializationRetriesExhausted(Exception):
    """Raised when retries ran out under contention (-> 409)."""


class DBConcurrencyService:
    def __init__(self, session: AsyncSession, max_retries: int = 3) -> None:
        self._session = session
        self._max_retries = max_retries

    async def open_account(
        self, account_id: str, owner: str, balance: int
    ) -> AccountResponse:
        account = Account(id=account_id, balance=balance, owner=owner)
        self._session.add(account)
        await self._session.commit()
        return AccountResponse.model_validate(account)

    async def get_account(self, account_id: str) -> AccountResponse:
        result = await self._session.execute(select(Account).where(id=account_id))
        account = result.scalar_one_or_none()
        if not account:
            raise HTTPException(detail="Account not found", status_code=404)
        return AccountResponse.model_validate(account)

    async def transfer(
        self, from_id: str, to_id: str, amount: int, isolation: IsolationLevel
    ):
        """
        Money transfer — a read-modify-write transaction whose correctness under
        concurrency depends on the isolation level.

        The body is intentionally the "natural" business logic: read both balances,
        check funds, debit one, credit the other. At READ COMMITTED two concurrent
        transfers from the same account can lose an update / overdraw. At SERIALIZABLE
        (the default here) Postgres aborts one of a conflicting pair with a
        serialization failure (40001), and we simply RETRY the whole transaction — so
        the intuitive code becomes correct without any manual locking.
        """
        isolation_sql = _ISOLATION_SQL
        for attempt in range(self._max_retries):
            try:
                async with self._session.begin():
                    # Must be the first statement in the transaction.
                    await self._session.execute(
                        text(f"SET TRANSACTION ISOLATION LEVEL {isolation_sql}")
                    )
                    result = await self._transfer_funds(from_id, to_id, amount)
                return TransferResponse(
                    from_id=from_id,
                    to_id=to_id,
                    amount=amount,
                    from_balance=result[0],
                    to_balance=result[1],
                    isolation_level=isolation,
                    retries=attempt,
                )
            except DBAPIError as exc:
                await self._session.rollback()
                sqlstate = getattr(getattr(exc, "orig", None), "sqlstate", None)
                if not is_serialization_failure(sqlstate):
                    raise
                if attempt > self._max_retries:
                    raise SerializationRetriesExhausted(from_id) from exc
                await asyncio.sleep(0.005 * (attempt + 1))

    async def _transfer_funds(
        self, from_id: str, to_id: str, amount: int
    ) -> Tuple[int, int]:
        from_balance = await self._session.scalar(_READ_BALANCE, {"id": from_id})
        to_balance = await self._session.scalar(_READ_BALANCE, {"id": to_id})
        if from_balance is None or to_balance is None:
            raise HTTPException(detail="Account not found", status_code=404)
        if from_balance < amount:
            raise InsufficientFunds(from_id)
        await self._session.execute(_DEBIT, {"id": from_id, "amt": amount})
        await self._session.execute(_CREDIT, {"id": to_id, "amt": amount})
        new_from_balance = await self._session.scalar(_READ_BALANCE, {"id": from_id})
        new_to_balance = await self._session.scalar(_READ_BALANCE, {"id": to_id})
        return new_from_balance, new_to_balance
