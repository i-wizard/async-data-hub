from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from typing import Optional

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, AsyncSession

from app.cache.redis_cache import AsyncCache
from app.config.settings import Settings
from app.utils.concurrency import ConcurrencyLimiter
from app.websocket.manager import WebSocketManager


@dataclass
class AppState:
    """
    Groups long-lived async resources so endpoints and services depend on a
    single, typed runtime container instead of reaching into global variables.
    """

    settings: Settings
    http_client: httpx.AsyncClient
    cache: AsyncCache
    db_engine: AsyncEngine
    session_factory: async_sessionmaker[AsyncSession]
    http_limiter: ConcurrencyLimiter
    webhook_limiter: ConcurrencyLimiter
    websocket_manager: WebSocketManager
    process_pool: Optional[ProcessPoolExecutor]
