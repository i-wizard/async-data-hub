from sqlalchemy import text

from app.cache.redis_cache import AsyncCache
from app.core.state import AppState
from app.schemas.health import HealthStatusResponse


class HealthService:
    """
    Verifies that core infrastructure is usable so readiness checks exercise the
    same async paths real requests depend on.
    """

    def __init__(self, state: AppState, cache: AsyncCache):
        """
        Receives shared resources from DI so health checks validate startup
        wiring rather than building independent one-off clients.
        """

        self._state = state
        self._cache = cache

    async def get_status(self) -> HealthStatusResponse:
        """
        Checks database and cache reachability because a healthy HTTP process is
        not enough if its downstream dependencies are unavailable.
        """

        database_status = "up"
        cache_status = "up"

        try:
            async with self._state.db_engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
        except Exception:
            database_status = "down"

        try:
            cache_ok = await self._cache.ping()
            if not cache_ok:
                cache_status = "down"
        except Exception:
            cache_status = "down"

        overall_status = "ok" if database_status == "up" and cache_status == "up" else "degraded"
        return HealthStatusResponse(
            status=overall_status,
            database=database_status,
            cache=cache_status,
        )
