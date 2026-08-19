"""
Drives the payment endpoint with deliberately racing requests so the idempotency
layer and the balance constraint can be observed under real concurrency.

Each scenario seeds its own customer directly in the database, fires its requests
with `asyncio.gather` (so they genuinely overlap), then reads the resulting rows
back to check the invariant that matters: money moved exactly as many times as it
should have, never more.

Run inside the api container so `app` imports and the database URL resolve:

    make simulate-payments

    # or
    docker compose exec api python -m scripts.payment.simulate_payment
"""

import asyncio
import uuid
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, List, Optional, Tuple

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config.settings import get_settings
from app.db.base import create_database_engine, create_session_factory
from app.db.models import Charge, CustomerAccount
from app.schemas.payment import ChargeStatus

BASE_URL = "http://127.0.0.1:8000"
ENDPOINT = "/api/v1/payments"
IDEMPOTENT_REPLAY_HEADER = "X-Idempotent-Replayed"
REQUEST_TIMEOUT_SECONDS = 30.0


@dataclass
class Call:
    """
    One simulated client request. `label` only exists to make the printed report
    readable when several racing calls return different outcomes.
    """

    label: str
    amount: int
    idempotency_key: str


@dataclass
class CallResult:
    """
    Captures a single call's outcome so a failure on one racing request never
    hides what the other request did.
    """

    label: str
    amount: int
    status_code: Optional[int]
    body: Any
    idempotent_replayed: bool = False
    error: Optional[str] = None

    @property
    def charged(self) -> bool:
        """
        True only when the API actually moved money. A rejected charge still
        returns 201 with a FAILED status, so the status code alone is not enough.
        """

        if self.status_code != 201 or not isinstance(self.body, dict):
            return False
        return self.body.get("status") == ChargeStatus.SUCCEEDED.value

    @property
    def summary(self) -> str:
        if self.error:
            return f"transport error: {self.error}"
        if isinstance(self.body, dict):
            detail = self.body.get("detail") or self.body.get("error_message")
            status = self.body.get("status", "-")
            replayed = " [replayed]" if self.idempotent_replayed else ""
            return f"HTTP {self.status_code} status={status}{replayed} detail={detail}"
        return f"HTTP {self.status_code} body={self.body}"


@lru_cache(maxsize=1)
def _session_factory() -> async_sessionmaker[AsyncSession]:
    """
    Builds the session factory once per process so every scenario shares a single
    engine and connection pool instead of opening a new one per database read.
    """

    settings = get_settings()
    engine = create_database_engine(settings.database_url)
    return create_session_factory(engine=engine)


async def _seed_customer(name: str, balance: int) -> str:
    """
    Creates a throwaway customer with a known balance so each scenario starts from
    a clean slate and its assertions are not affected by earlier runs.
    """

    session_factory = _session_factory()
    async with session_factory() as session:
        customer = CustomerAccount(name=name, balance=balance)
        session.add(customer)
        await session.commit()
        return str(customer.id)


async def _account_snapshot(customer_id: str) -> Tuple[int, List[Charge]]:
    """
    Reads the customer's balance and every charge row written for them, which is
    the ground truth used to verify what the racing requests really did.
    """

    account_id = uuid.UUID(customer_id)
    session_factory = _session_factory()
    async with session_factory() as session:
        balance = await session.scalar(
            select(CustomerAccount.balance).where(CustomerAccount.id == account_id)
        )
        charges = await session.scalars(
            select(Charge)
            .where(Charge.customer_id == account_id)
            .order_by(Charge.created_at)
        )
        return balance, list(charges)


async def _charge(client: httpx.AsyncClient, call: Call, customer_id: str) -> CallResult:
    """
    Issues one payment request and never raises, so a single rejected or failed
    call cannot abort the sibling call it is racing against.
    """

    try:
        response = await client.post(
            ENDPOINT,
            json={"amount": call.amount, "customer_id": customer_id},
            headers={"Idempotency-Key": call.idempotency_key},
        )
    except httpx.HTTPError as exc:
        return CallResult(
            label=call.label,
            amount=call.amount,
            status_code=None,
            body=None,
            error=str(exc),
        )

    try:
        body = response.json()
    except ValueError:
        body = response.text

    return CallResult(
        label=call.label,
        amount=call.amount,
        status_code=response.status_code,
        body=body,
        idempotent_replayed=response.headers.get(IDEMPOTENT_REPLAY_HEADER) == "true",
    )


async def _fire_concurrently(customer_id: str, calls: List[Call]) -> List[CallResult]:
    """
    Sends every call at once through one client so the requests overlap on the
    server and the idempotency race is real rather than sequential.
    """

    async with httpx.AsyncClient(
        base_url=BASE_URL, timeout=REQUEST_TIMEOUT_SECONDS
    ) as client:
        tasks = [
            _charge(client=client, call=call, customer_id=customer_id)
            for call in calls
        ]
        return await asyncio.gather(*tasks)


async def _report(
    title: str,
    expectation: str,
    customer_id: str,
    starting_balance: int,
    expected_charges: int,
    results: List[CallResult],
) -> bool:
    """
    Prints what each racing call returned next to the durable database state and
    verifies the money invariant: the balance must have moved by exactly the sum
    of the calls that reported a successful charge, and no more.
    """

    balance, charges = await _account_snapshot(customer_id=customer_id)
    succeeded_charges = [c for c in charges if c.status == ChargeStatus.SUCCEEDED.value]
    charged_calls = [r for r in results if r.charged]
    expected_balance = starting_balance - sum(r.amount for r in charged_calls)

    print("=" * 72)
    print(title)
    print(f"expected: {expectation}")
    print("-" * 72)
    for result in results:
        print(f"  {result.label:<8} amount={result.amount:<5} {result.summary}")
    print(f"  balance  : {starting_balance} -> {balance}")
    print(f"  charges  : {len(succeeded_charges)} succeeded, {len(charges)} total")
    for charge in charges:
        print(
            f"    {charge.id} {charge.status:<9} amount={charge.amount} "
            f"key={charge.idempotency_key} error={charge.error_message}"
        )

    checks = {
        "successful charges match expectation": len(succeeded_charges)
        == expected_charges,
        "API successes match charge rows": len(charged_calls)
        == len(succeeded_charges),
        "balance debited exactly once per success": balance == expected_balance,
    }
    for description, passed in checks.items():
        print(f"  [{'PASS' if passed else 'FAIL'}] {description}")
    print("=" * 72)
    return all(checks.values())


def _new_key() -> str:
    return f"sim_{uuid.uuid4().hex}"


async def concurrent_payment_with_same_idempotency_key_and_same_payload() -> bool:
    "same user with same key, one should succeed, the other should be idempotent replay or fail"

    starting_balance = 500
    customer_id = await _seed_customer(
        name="sim-same-key-same-payload", balance=starting_balance
    )
    idempotency_key = _new_key()
    calls = [
        Call(label="first", amount=100, idempotency_key=idempotency_key),
        Call(label="second", amount=100, idempotency_key=idempotency_key),
    ]

    results = await _fire_concurrently(customer_id=customer_id, calls=calls)
    return await _report(
        title="1. same idempotency key, same payload, concurrent",
        expectation=(
            "one 201 SUCCEEDED; the other replays it or is rejected with 409 "
            "while the first is still in flight"
        ),
        customer_id=customer_id,
        starting_balance=starting_balance,
        expected_charges=1,
        results=results,
    )

async def concurrent_payment_with_same_idempotency_key_for_an_already_successful_payment():
    "same user with same key, one should succeed, the other should be idempotent replay"

    starting_balance = 500
    customer_id = await _seed_customer(
        name="sim-same-key-same-payload", balance=starting_balance
    )
    idempotency_key = _new_key()
    calls = [
        Call(label="first", amount=100, idempotency_key=idempotency_key),
        Call(label="second", amount=100, idempotency_key=idempotency_key),
    ]

    # Fire the first call to create a successful payment
    await _fire_concurrently(customer_id=customer_id, calls=[calls[0]])

    # Fire the second call to test idempotent replay
    results = await _fire_concurrently(customer_id=customer_id, calls=[calls[1]])
    return await _report(
        title="6. same idempotency key for an already successful payment",
        expectation=(
            "the second call replays the first successful payment"
        ),
        customer_id=customer_id,
        starting_balance=starting_balance,
        expected_charges=1,
        results=results,
    )

async def concurrent_payment_with_same_idempotency_key_with_different_payload() -> bool:
    "same user with same key, different payload, one should succeed, the other should fail"

    starting_balance = 500
    customer_id = await _seed_customer(
        name="sim-same-key-different-payload", balance=starting_balance
    )
    idempotency_key = _new_key()
    calls = [
        Call(label="first", amount=100, idempotency_key=idempotency_key),
        Call(label="second", amount=250, idempotency_key=idempotency_key),
    ]

    results = await _fire_concurrently(customer_id=customer_id, calls=calls)
    return await _report(
        title="2. same idempotency key, different payload, concurrent",
        expectation=(
            "one 201 SUCCEEDED; the other is rejected with 409 because the "
            "payload fingerprint does not match the key"
        ),
        customer_id=customer_id,
        starting_balance=starting_balance,
        expected_charges=1,
        results=results,
    )


async def concurrent_payment_with_different_idempotency_keys() -> bool:
    "same user should succeed"

    starting_balance = 500
    customer_id = await _seed_customer(
        name="sim-different-keys", balance=starting_balance
    )
    calls = [
        Call(label="first", amount=100, idempotency_key=_new_key()),
        Call(label="second", amount=150, idempotency_key=_new_key()),
    ]

    results = await _fire_concurrently(customer_id=customer_id, calls=calls)
    return await _report(
        title="3. different idempotency keys, different amounts, concurrent",
        expectation="both 201 SUCCEEDED — distinct keys are distinct operations",
        customer_id=customer_id,
        starting_balance=starting_balance,
        expected_charges=2,
        results=results,
    )


async def concurrent_same_user_with_different_idempotency_keys_and_same_payload() -> bool:
    "same user should succeed"

    starting_balance = 500
    customer_id = await _seed_customer(
        name="sim-different-keys-same-payload", balance=starting_balance
    )
    calls = [
        Call(label="first", amount=100, idempotency_key=_new_key()),
        Call(label="second", amount=100, idempotency_key=_new_key()),
    ]

    results = await _fire_concurrently(customer_id=customer_id, calls=calls)
    return await _report(
        title="4. different idempotency keys, identical payload, concurrent",
        expectation=(
            "both 201 SUCCEEDED and the balance drops twice — deduplication is "
            "keyed on the header, not on the request body"
        ),
        customer_id=customer_id,
        starting_balance=starting_balance,
        expected_charges=2,
        results=results,
    )


async def concurrent_same_user_with_two_payment_amount_whose_sum_should_exceed_limit() -> bool:
    "one should succeed, the other should fail"

    starting_balance = 500
    customer_id = await _seed_customer(
        name="sim-exceeds-balance", balance=starting_balance
    )
    calls = [
        Call(label="first", amount=300, idempotency_key=_new_key()),
        Call(label="second", amount=300, idempotency_key=_new_key()),
    ]

    results = await _fire_concurrently(customer_id=customer_id, calls=calls)
    return await _report(
        title="5. two concurrent charges whose sum exceeds the balance",
        expectation=(
            "one 201 SUCCEEDED; the other returns a FAILED charge with "
            "'Insufficient balance' because the row-level check constraint holds"
        ),
        customer_id=customer_id,
        starting_balance=starting_balance,
        expected_charges=1,
        results=results,
    )


SCENARIOS = [
    # concurrent_payment_with_same_idempotency_key_and_same_payload,
    # concurrent_payment_with_same_idempotency_key_with_different_payload,
    # concurrent_payment_with_different_idempotency_keys,
    # concurrent_same_user_with_different_idempotency_keys_and_same_payload,
    # concurrent_same_user_with_two_payment_amount_whose_sum_should_exceed_limit,
concurrent_payment_with_same_idempotency_key_for_an_already_successful_payment
]


async def run_all() -> None:
    """
    Runs every scenario in sequence — never concurrently — so the printed report
    stays readable and one scenario's load cannot perturb another's timing.
    """

    outcomes = []
    for scenario in SCENARIOS:
        passed = await scenario()
        outcomes.append((scenario.__name__, passed))

    print()
    print("SUMMARY")
    for name, passed in outcomes:
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}")


if __name__ == "__main__":
    asyncio.run(run_all())
