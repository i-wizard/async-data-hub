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
from app.schemas.inventory import ProductResponse, ProductReservationResponse, ReserveStrategy

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
            raise HTTPException(detail="Product not found", status_code=status.HTTP_404_NOT_FOUND)
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
        await self._session.flush() # Flush to get the ID and created_at of the new reservation without committing yet
        return ProductReservationResponse.model_validate(reservation)

    async def reserve_product(self, product_id: str, quantity: int, strategy: ReserveStrategy):
        handlers = {
            ReserveStrategy.NAIVE: self.reserve_naive,
        }
        return await handlers[strategy](product_id, quantity)

    async def reserve_naive(self, product_id: str, quantity: int):
        async with self._session.begin():
            product = await self._session.get(Product, product_id)
            if not product:
                raise HTTPException(detail="Product not found", status_code=status.HTTP_404_NOT_FOUND)
            if product.stock < quantity:
                raise HTTPException(detail="Not enough stock", status_code=status.HTTP_400_BAD_REQUEST)
            await asyncio.sleep(RACE_WINDOW_SECONDS)
            await self._session.execute(
                update(Product)
                .where(Product.id == product_id)
                .values(stock=product.stock - quantity)
            )
            return await self._record_reservation(product_id=product_id, quantity=quantity)
