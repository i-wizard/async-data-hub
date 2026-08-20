"""
Inventory reservation — the same operation implemented five ways so the
concurrency-control techniques can be compared directly.

The operation is always: "reserve `qty` units of a product if enough stock
remains, decrement the stock, and record a Reservation row." The ONLY thing
that differs is how each strategy stays correct when many requests race for the
last units.

Strategies
  naive       read stock, (pause), write stock-qty          -> LOST UPDATE (oversells)
  optimistic  read (stock, version); UPDATE ... WHERE version=v; retry on miss
  pessimistic SELECT ... FOR UPDATE (row lock), then decrement
  atomic      one UPDATE ... SET stock=stock-qty WHERE stock>=qty (DB does CAS)
  locked      hold a Redis distributed lock around a read-modify-write
"""

import asyncio

from fastapi import HTTPException, status
from sqlalchemy import select, func, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.cache.redis_cache import AsyncCache
from app.db.models import Product, ProductReservation
from app.schemas.inventory import (
    ProductResponse,
    ProductReservationResponse,
    ReserveStrategy,
)

# An artificial pause between read and write, used ONLY by the racy strategies
# (naive, locked) to widen the race window so demos reliably show the effect.
RACE_WINDOW_SECONDS = 0.1
# Optimistic locking retries a bounded number of times before giving up.
MAX_OPTIMISTIC_ATTEMPTS = 5


class InventoryService:
    def __init__(self, session: AsyncSession, cache: AsyncCache):
        self._session = session
        self._cache = cache

    async def add_product(self, name: str, stock: int):
        product = Product(name=name, stock=stock)
        self._session.add(product)
        await self._session.commit()
        return ProductResponse.model_validate(product)

    async def list_products(self, limit: int = 100, offset: int = 0):
        statement = select(Product).limit(limit).offset(offset)
        result = await self._session.execute(statement)
        product = result.scalars()
        return [ProductResponse.model_validate(p) for p in product]

    async def get_product(self, product_id: str):
        product = await self._session.get(Product, product_id)
        if not product:
            raise HTTPException(
                detail="Product not found", status_code=status.HTTP_404_NOT_FOUND
            )
        return ProductResponse.model_validate(product)

    async def count_reservations(self, product_id: str):
        statement = (
            select(func.count())
            .select_from(ProductReservation)
            .where(ProductReservation.product_id == product_id)
        )
        result = await self._session.execute(statement)
        count = result.scalar_one()
        return count

    async def _record_reservation(self, product_id: str, quantity: int):
        reservation = ProductReservation(product_id=product_id, quantity=quantity)
        self._session.add(reservation)
        await self._session.flush()  # Flush to get the ID and created_at of the new reservation without committing yet
        return ProductReservationResponse.model_validate(reservation)

    async def reserve_product(
        self, product_id: str, quantity: int, strategy: ReserveStrategy
    ):
        handlers = {
            ReserveStrategy.NAIVE: self._reserve_naive,
            ReserveStrategy.OPTIMISTIC: self._reserve_optimistic,
            ReserveStrategy.PESSIMISTIC: self._reserve_pessimistic,
        }
        return await handlers[strategy](product_id, quantity)

    async def _reserve_naive(self, product_id: str, quantity: int):
        async with self._session.begin():
            product = await self._session.get(Product, product_id)
            if not product:
                raise HTTPException(
                    detail="Product not found", status_code=status.HTTP_404_NOT_FOUND
                )
            if product.stock < quantity:
                raise HTTPException(
                    detail="Not enough stock", status_code=status.HTTP_400_BAD_REQUEST
                )
            await asyncio.sleep(RACE_WINDOW_SECONDS)
            await self._session.execute(
                update(Product)
                .where(Product.id == product_id)
                .values(stock=product.stock - quantity)
            )
            return await self._record_reservation(
                product_id=product_id, quantity=quantity
            )

    async def _reserve_optimistic(self, product_id: str, quantity: int):
        """
        Check and set (CAS) on a version column, retrying on a lost race
        Read (stock, version); Attempt update WHERE version=v; retry on miss.
        Great when contention is rare, but can fail under high concurrency.
        Increasing the MAX_OPTIMISTIC_ATTEMPTS and/or backoff delay can help, but if the contention is too high, the system may need a different strategy (pessimistic locking, atomic update, or distributed lock).
        """
        for attempt in range(MAX_OPTIMISTIC_ATTEMPTS):
            async with self._session.begin():
                row = (
                    await self._session.execute(
                        select(Product.stock, Product.version).where(
                            Product.id == product_id
                        )
                    )
                ).one_or_none()
                if row is None:
                    raise HTTPException(
                        detail="Product not found",
                        status_code=status.HTTP_404_NOT_FOUND,
                    )
                stock, version = row
                if stock < quantity:
                    raise HTTPException(
                        detail="Not enough stock",
                        status_code=status.HTTP_400_BAD_REQUEST,
                    )
                await asyncio.sleep(RACE_WINDOW_SECONDS)
                result = await self._session.execute(
                    update(Product)
                    .where(Product.id == product_id, Product.version == version)
                    .values(stock=stock - quantity, version=self._next_version(version))
                )
                if result.rowcount == 1:
                    # We won the CAS: record the reservation and commit.
                    return await self._record_reservation(
                        product_id=product_id, quantity=quantity
                    )
                # rowcount == 0: version changed under us -> this transaction is a
                # no-op; it commits, and we retry with a fresh read.
            if self._should_retry(attempt, MAX_OPTIMISTIC_ATTEMPTS):
                delay = self._backoff_delay(attempt)
                await asyncio.sleep(delay)
                print(
                    f"Optimistic retry {attempt + 1}/{MAX_OPTIMISTIC_ATTEMPTS} after {delay:.3f}s backoff"
                )
            else:
                break
        raise HTTPException(
            detail="Too many concurrent attempts; please try again",
            status_code=status.HTTP_409_CONFLICT,
        )

    async def _reserve_pessimistic(self, product_id: str, quantity: int):
        """
        Lock the row up front with SELECT ... FOR UPDATE.

        The lock is held until commit, so concurrent reservers BLOCK at their own
        SELECT FOR UPDATE and then read the already-decremented stock. Correct and
        simple; best when contention is HIGH (avoids wasted optimistic retries),
        at the cost of holding locks and reduced concurrency.
        Works better than the optimistic strategy because the db immediately processes the queued transaction as soon as the lock is free instead of waiting for the next retry attempt.
        Works better than the optimistic strategy when there are many concurrent requests for the same product because it avoids the overhead of retries and backoff delays (the db handles all this for us).
        ARGS for with_for_update
        - skip_locked=True: If the row is already locked by another transaction, skip it and grab the nex available row matching the query instead of waiting. Will return None if the row is locked since there is only one row per product_id.
        - nowait=True: If the row is already locked by another transaction, raise a asyncpg.exceptions.LockNotAvailableError  instead of waiting. This is useful if you want to fail fast and handle the error in your application logic.
        """
        async with self._session.begin():
            product = (
                await self._session.execute(
                    select(Product).where(Product.id == product_id).with_for_update()
                )
            ).scalar_one_or_none()
            if not product:
                raise HTTPException(
                    detail="Product not found", status_code=status.HTTP_404_NOT_FOUND
                )
            if product.stock < quantity:
                raise HTTPException(
                    detail="Not enough stock", status_code=status.HTTP_400_BAD_REQUEST
                )
            await asyncio.sleep(RACE_WINDOW_SECONDS)
            product.stock -= quantity
            return await self._record_reservation(
                product_id=product_id, quantity=quantity
            )

    @staticmethod
    def _next_version(version: int):
        return version + 1

    @staticmethod
    def _backoff_delay(
        attempt: int, base: float = 0.01, factor: float = 2.0, max_delay: float = 1.0
    ) -> float:
        """
        Deterministic exponential backoff for retry `attempt` (0-indexed), in seconds.

        delay = min(max_delay, base * factor**attempt). Real systems ALSO add random
        jitter to avoid a thundering herd of synchronized retries
        """
        return min(max_delay, base * (factor**attempt))

    @staticmethod
    def _should_retry(attempt: int, max_attempt: int):
        """
        Return True if another attempt is allowed after this 0-indexed `attempt`.
        """
        return attempt + 1 < max_attempt
