import asyncio
import hashlib
import json
import uuid

from fastapi import HTTPException
from sqlalchemy import update, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.cache.redis_cache import AsyncCache
from app.db.models import CustomerAccount, Charge
from app.schemas.payment import (
    CreatePaymentRequest,
    PaymentResponse,
    ChargeStatus,
    PaymentRequestProcessingStatus,
)
from app.utils.logger import CustomLogger

_BALANCE_CHECK_CONSTRAINT = "ck_customer_accounts_balance_non_negative"
_IDEMPOTENCY_UNIQUE_INDEX = "uq_charges_idempotency_key_succeeded"


def _constraint_name(exc: IntegrityError) -> str:
    """
    Extract the DB constraint name from an IntegrityError across drivers.

    asyncpg exposes ``constraint_name`` directly on the wrapped exception;
    psycopg surfaces it via ``diag.constraint_name``. Fall back to the
    stringified error, which contains the constraint name on both drivers.
    """
    orig = getattr(exc, "orig", None)
    name = getattr(orig, "constraint_name", None)
    if name:
        return name
    diag = getattr(orig, "diag", None)
    name = getattr(diag, "constraint_name", None)
    if name:
        return name
    return str(orig or exc)


class PaymentService:
    _KEY_PREFIX = "idemp:"

    def __init__(self, session: AsyncSession, cache: AsyncCache):
        self._session = session
        self._cache = cache
        self.processing_record_ttl_seconds = 60

    @staticmethod
    def _to_uuid(customer_id: str) -> uuid.UUID:
        # Convert the customer_id string to a UUID object
        try:
            return uuid.UUID(customer_id)
        except (ValueError, TypeError):
            raise ValueError(f"Invalid customer_id: {customer_id}")

    async def charge_with_bug(self, data: CreatePaymentRequest, idempotency_key: str, bug: str):
        if bug == "replay":
            result = await self.charge_count_with_replay_bug(data, idempotency_key)
            return result.model_dump(mode="json")

    async def charge_count_with_replay_bug(
        self, data: CreatePaymentRequest, idempotency_key: str
    ) -> PaymentResponse:
        """sending two concurrent requests should succeed"""
        customer_id = self._to_uuid(customer_id=data.customer_id)
        charge = Charge(
            amount=data.amount,
            customer_id=customer_id,
            status=ChargeStatus.SUCCEEDED,
            idempotency_key=idempotency_key,
        )
        # `FOR UPDATE` locks the account row for the life of this transaction, so a
        # concurrent request for the same customer blocks here until we commit and
        # then reads the already-debited balance instead of the stale one.
        print("Started processing charge with replay bug")
        async with self._session.begin():
            statement = (
                select(CustomerAccount)
                .where(CustomerAccount.id == customer_id)
            )
            # statement = (
            #     select(CustomerAccount)
            #     .where(CustomerAccount.id == customer_id)
            #     .with_for_update()
            # )
            result = await self._session.execute(statement)
            customer_account = result.scalar_one_or_none()
            if customer_account is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Customer with id {data.customer_id} not found.",
                )
            print(f"Customer {customer_id} balance before charge: {customer_account.balance}")
            if customer_account.balance < data.amount:
                charge.status = ChargeStatus.FAILED
                charge.error_message = "Insufficient balance"
                self._session.add(charge)
                # Flush so the column defaults (id, created_at) exist before the
                # response is built; the enclosing block still commits on exit.
                await self._session.flush()
                return PaymentResponse.model_validate(charge)
            # Held inside the transaction on purpose: the lock stays taken for the
            # whole delay, which is what makes the serialization observable.
            await asyncio.sleep(2)  # Simulate network delay
            customer_account.balance -= data.amount
            self._session.add(charge)
        print(f"Customer {customer_id} balance after charge: {customer_account.balance}")
        return PaymentResponse.model_validate(charge)

    async def charge_account(
        self, data: CreatePaymentRequest, idempotency_key: str
    ) -> PaymentResponse:
        """
        Debit a customer's account and record the corresponding charge atomically.

        Both the balance update and the charge insert are performed inside a
        single transaction so a failure on either side leaves no partial state.
        If a request is replayed with same idempotency key as a previous successful request, the db unique constraint on idempotency_key will prevent a second SUCCEEDED charge from being created.
        """
        customer_id = self._to_uuid(customer_id=data.customer_id)
        charge = Charge(
            amount=data.amount,
            customer_id=customer_id,
            status=ChargeStatus.SUCCEEDED,
            idempotency_key=idempotency_key,
        )
        await asyncio.sleep(2) # simulate network delay
        try:
            async with self._session.begin():
                statement = (
                    update(CustomerAccount)
                    .where(CustomerAccount.id == customer_id)
                    .values(balance=CustomerAccount.balance - data.amount)
                )
                result = await self._session.execute(statement)
                if result.rowcount == 0:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Customer with id {data.customer_id} not found.",
                    )

                self._session.add(charge)
        except IntegrityError as exc:
            # The transaction has already rolled back; figure out which
            # constraint fired so we can respond appropriately.
            constraint = _constraint_name(exc)
            if _BALANCE_CHECK_CONSTRAINT in constraint:
                charge.status = ChargeStatus.FAILED
                charge.error_message = "Insufficient balance"
                self._session.add(charge)
                await self._session.commit()
                return PaymentResponse.model_validate(charge)
            if _IDEMPOTENCY_UNIQUE_INDEX in constraint:
                # A SUCCEEDED charge with this idempotency_key already exists.
                # The idempotency layer should normally intercept replays before
                # we get here, so surface a conflict rather than silently retrying.
                raise HTTPException(
                    status_code=409,
                    detail="A successful charge for this idempotency key already exists.",
                ) from exc
            CustomLogger.error(f"Unexpected integrity error on charge: {exc}")
            raise

        return PaymentResponse.model_validate(charge)

    @staticmethod
    def _compute_fingerprint(data: dict) -> str:
        """
        Compute a fingerprint of the request data to detect if the same operation
        is being retried with identical parameters.
        """
        # For simplicity, we can use a hash of the sorted items in the data dict.
        # In production, consider using a more robust method (e.g., JSON canonicalization).

        serialized_data = json.dumps(data, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(serialized_data.encode()).hexdigest()

    def _cache_idempotency_key(self, idempotency_key: str) -> str:
        return f"{self._KEY_PREFIX}{idempotency_key}"

    async def charge_with_idempotency(
        self, data: CreatePaymentRequest, idempotency_key: str
    ) -> dict:
        """
        Charge a customer's account with idempotency handling.
        """
        customer_id = data.customer_id
        amount = data.amount
        payload = {"customer_id": customer_id, "amount": amount}
        fingerprint = self._compute_fingerprint(payload)
        cache_key = self._cache_idempotency_key(idempotency_key)
        processing_record = json.dumps(
            {
                "status": PaymentRequestProcessingStatus.processing.value,
                "fingerprint": fingerprint,
            }
        )
        acquired = await self._cache.setnx(
            key=cache_key,
            value=processing_record,
            ttl_seconds=self.processing_record_ttl_seconds,
        )
        if not acquired:
            return await self._handle_existing(cache_key, fingerprint)

        # --- We are the first request: perform the side effect exactly once. ---
        CustomLogger.info(
            f"Acquired idempotency key %s; executing operation {idempotency_key}"
        )
        try:
            response = await self.charge_account(data, idempotency_key)
            response = response.model_dump(mode="json")
        except Exception:
            # The operation failed, so nothing was durably done from the client's
            # point of view. Release the key so a genuine retry can proceed
            # instead of being blocked as "in flight" for the whole TTL.
            await self._cache.delete(cache_key)
            CustomLogger.error(
                f"Operation failed for key %s; released the key {idempotency_key}"
            )
            raise
        completed_record = json.dumps(
            {
                "status": PaymentRequestProcessingStatus.completed.value,
                "fingerprint": fingerprint,
                "response": response,
            }
        )
        await self._cache.set(
            key=cache_key,
            value=completed_record,
            ttl_seconds=self.processing_record_ttl_seconds,
        )
        return response

    async def _handle_existing(self, cache_key: str, fingerprint: str):
        existing_record_json = await self._cache.get(cache_key)
        if not existing_record_json:
            # extremely rare case: the key was deleted between the setnx and get calls
            # raise so that client can retry
            raise HTTPException(
                status_code=409,
                detail="Something went wrong. Please retry the request.",
            )

        existing_record = json.loads(existing_record_json)
        existing_fingerprint = existing_record.get("fingerprint")
        status = existing_record.get("status")

        if existing_fingerprint != fingerprint:
            raise HTTPException(
                status_code=409,
                detail="Idempotency key conflict: different request payload for the same key.",
            )

        if status == PaymentRequestProcessingStatus.processing.value:
            raise HTTPException(
                status_code=409,
                detail="Request is still being processed. Please wait and retry.",
            )

        # If the status is completed, we can replay the response
        response = existing_record.get("response")
        response["idempotent_replayed"] = True
        return response
