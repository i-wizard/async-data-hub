import asyncio
import time
import uuid

from app.cache.redis_cache import AsyncCache


class LockUnavailable(Exception):
    """Raised when the lock could not be acquired within the timeout."""

class DistributedLock:
    def __init__(self, cache: AsyncCache, lock_name, ttl_seconds=50):
        self.cache = cache
        self.lock_name = lock_name
        self.ttl_seconds = ttl_seconds
        self.lock_token = uuid.uuid4().hex

    async def acquire(self, timeout_seconds=5, poll_interval=0.02) -> bool:
        """
        Block until the lock is acquired or `timeout_seconds` seconds elapse.
        The minimum acquisition time is `poll_interval` seconds, so set it low enough to avoid unnecessary delays.
        """
        deadline = time.monotonic() + timeout_seconds
        while True:
            if await self.cache.setnx(
                self.lock_name, self.lock_token, self.ttl_seconds
            ):
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(poll_interval)

    async def release(self) -> None:
        """
        Release the lock if we still hold it (atomic token check + delete).
        This is safe even if the lock has expired and been acquired by another client.
        """
        await self.cache.eval(
            """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            else
                return 0
            end
            """,
            1,
            self.lock_name,
            self.lock_token,
        )

    async def __aenter__(self):
        if not await self.acquire():
            raise LockUnavailable(f"Could not acquire lock {self.lock_name} in time")
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.release()
