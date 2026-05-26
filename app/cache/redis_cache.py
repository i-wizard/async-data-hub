import asyncio
import json
import time
from typing import Any, Dict, Optional

from redis.asyncio import Redis

from app.cache.request_coalescer import RequestCoalescer


class AsyncCache:
    """
    Wraps Redis with an in-memory fallback so the rest of the code can focus on
    cache semantics without being tightly coupled to one backend implementation.
    """

    def __init__(
        self,
        redis_client: Optional[Redis],
        allow_in_memory_fallback: bool,
        default_ttl_seconds: int,
    ):
        """
        Stores both the backend client and fallback policy because tests and
        local bootstrapping often need a lighter-weight cache mode.
        """

        self._redis_client = redis_client
        self._allow_in_memory_fallback = allow_in_memory_fallback
        self._default_ttl_seconds = default_ttl_seconds
        self._memory_store: Dict[str, Dict[str, Any]] = {}
        self._coalescer = RequestCoalescer()
        self._lock = asyncio.Lock()

    async def get(self, key: str) -> Optional[Any]:
        """
        Reads cached JSON payloads through one interface so callers do not care
        whether the value came from Redis or the in-memory fallback.
        """

        if self._redis_client is not None:
            value = await self._redis_client.get(key)
            return json.loads(value) if value is not None else None

        if not self._allow_in_memory_fallback:
            return None

        async with self._lock:
            record = self._memory_store.get(key)
            if record is None or record["expires_at"] < time.time():
                self._memory_store.pop(key, None)
                return None
            return record["value"]

    async def set(self, key: str, value: Any, ttl_seconds: Optional[int] = None) -> None:
        """
        Writes cache entries with explicit TTL handling because stale aggregate
        data should expire predictably regardless of backend.
        """

        ttl = ttl_seconds or self._default_ttl_seconds

        if self._redis_client is not None:
            await self._redis_client.set(name=key, value=json.dumps(value), ex=ttl)
            return

        if not self._allow_in_memory_fallback:
            return

        async with self._lock:
            self._memory_store[key] = {"value": value, "expires_at": time.time() + ttl}

    async def delete(self, key: str) -> None:
        """
        Removes cache entries so tests and cache-bypass flows can force a fresh
        aggregation without reaching into backend-specific APIs.
        """

        if self._redis_client is not None:
            await self._redis_client.delete(key)
            return

        async with self._lock:
            self._memory_store.pop(key, None)

    async def get_or_set(self, key: str, factory, ttl_seconds: Optional[int] = None) -> Any:
        """
        Combines cache lookup with request coalescing so duplicate in-flight work
        is collapsed before it can amplify downstream load.
        """

        cached_value = await self.get(key)
        if cached_value is not None:
            return cached_value

        async def _build() -> Any:
            cached_inner = await self.get(key)
            if cached_inner is not None:
                return cached_inner

            value = await factory()
            await self.set(key=key, value=value, ttl_seconds=ttl_seconds)
            return value

        return await self._coalescer.run(key=key, factory=_build)

    async def ping(self) -> bool:
        """
        Exposes a backend-agnostic health check because readiness endpoints need
        to validate cache access without duplicating cache internals.
        """

        if self._redis_client is not None:
            response = await self._redis_client.ping()
            return bool(response)
        return self._allow_in_memory_fallback

    async def aclose(self) -> None:
        """
        Shuts down the Redis client cleanly so startup-created sockets do not
        leak when the application process exits.
        """

        if self._redis_client is not None:
            await self._redis_client.aclose()
