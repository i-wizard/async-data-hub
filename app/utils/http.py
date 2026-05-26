import asyncio
import time
from typing import Dict, Optional

import httpx

from app.utils.concurrency import ConcurrencyLimiter, preserve_cancellation
from app.utils.logger import CustomLogger


class SharedHttpClient:
    """
    Adds limiter, timeout, and error-shaping behavior around one shared client
    so outbound calls remain consistent across the whole application.
    """

    def __init__(self, client: httpx.AsyncClient, limiter: ConcurrencyLimiter):
        """
        Stores the shared client and limiter because outbound fan-out must reuse
        process-wide resources to avoid connection and task explosions.
        """

        self._client = client
        self._limiter = limiter

    async def get_json(self, url: str, params: Optional[Dict[str, str]] = None) -> Dict:
        """
        Fetches JSON through the concurrency limiter so aggregate fan-out stays
        bounded even when many requests hit the service simultaneously.
        """

        async def _request() -> Dict:
            started_at = time.perf_counter()
            try:
                response = await self._client.get(url=url, params=params)
                response.raise_for_status()
                return response.json()
            except asyncio.CancelledError:
                raise
            except httpx.HTTPError as exc:
                CustomLogger.warning(
                    "outbound_http_failure",
                    extra={"url": url, "error": str(exc)},
                )
                raise RuntimeError(str(exc)) from exc
            finally:
                duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
                CustomLogger.info(
                    "outbound_http_completed",
                    extra={"url": url, "duration_ms": duration_ms},
                )

        return await preserve_cancellation(self._limiter.run(_request()))

    async def post_json(self, url: str, payload: Dict) -> httpx.Response:
        """
        Sends JSON POST requests through the shared limiter so webhook fan-out
        follows the same bounded-outbound behavior as aggregation calls.
        """

        async def _request() -> httpx.Response:
            try:
                response = await self._client.post(url=url, json=payload)
                response.raise_for_status()
                return response
            except asyncio.CancelledError:
                raise
            except httpx.HTTPError as exc:
                raise exc

        return await preserve_cancellation(self._limiter.run(_request()))
