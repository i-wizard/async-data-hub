import asyncio
from typing import Any, Awaitable, Callable, Dict


class RequestCoalescer:
    """
    Deduplicates identical in-flight work so a burst of matching requests does
    not trigger redundant upstream fan-out before the cache is populated.
    """

    def __init__(self):
        """
        Keeps an in-memory task registry because deduplication only needs to
        protect concurrent work within the current application process.
        """

        self._tasks: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    async def run(self, key: str, factory: Callable[[], Awaitable[Any]]) -> Any:
        """
        Shares one task among concurrent callers for the same key so the first
        request does the work and followers await its result.
        """

        async with self._lock:
            task = self._tasks.get(key)
            if task is None:
                task = asyncio.create_task(factory())
                self._tasks[key] = task

        try:
            return await task
        finally:
            if task.done():
                async with self._lock:
                    if self._tasks.get(key) is task:
                        self._tasks.pop(key, None)
