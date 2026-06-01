import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator


class StreamCounter:
    """
    Tracks active streaming connections across endpoints so runtime state can
    answer how many clients are currently consuming streamed responses.
    """

    def __init__(self):
        """
        Keeps the count and lock together because streaming responses may enter
        or exit from different request tasks at the same time.
        """

        self._count = 0
        self._lock = asyncio.Lock()

    async def get(self) -> int:
        """
        Reads the current count under the lock so the endpoint never observes a
        partially updated value while stream tasks are entering or exiting.
        """

        async with self._lock:
            return self._count

    @asynccontextmanager
    async def track(self) -> AsyncIterator[None]:
        """
        Increments on stream iteration start and decrements on exit so normal
        completion, exceptions, and client disconnects all release the count.
        """

        async with self._lock:
            self._count += 1
        try:
            yield
        finally:
            async with self._lock:
                self._count -= 1
