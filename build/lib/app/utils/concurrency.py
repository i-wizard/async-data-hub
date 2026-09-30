import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator, Awaitable, TypeVar

T = TypeVar("T")


class ConcurrencyLimiter:
    """
    Centralizes semaphore-based limiting so downstream concurrency caps stay
    explicit and reusable across HTTP fan-out, webhooks, and other workloads.
    """

    def __init__(self, limit: int):
        """
        Stores the semaphore at object scope because concurrency limits should be
        shared across requests, not recreated per call.
        """

        self._semaphore = asyncio.Semaphore(limit)

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        """
        Exposes limiter acquisition as a context manager so call sites remain
        readable while still guaranteeing release during exceptions.
        """

        async with self._semaphore:
            yield

    async def run(self, task: Awaitable[T]) -> T:
        """
        Runs one awaitable under the semaphore so the limiter can be used in
        service code without repetitive context-manager boilerplate.
        """

        async with self.slot():
            return await task


async def preserve_cancellation(awaitable: Awaitable[T]) -> T:
    """
    Re-raises CancelledError intentionally because async learning projects often
    fail by swallowing cancellation and leaving orphaned work behind.
    """

    try:
        return await awaitable
    except asyncio.CancelledError:
        raise
