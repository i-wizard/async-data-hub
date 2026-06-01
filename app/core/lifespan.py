from contextlib import asynccontextmanager
from concurrent.futures import ProcessPoolExecutor
from typing import AsyncIterator

import httpx
from fastapi import FastAPI
from redis.asyncio import Redis

from app.cache.redis_cache import AsyncCache
from app.config.settings import get_settings
from app.core.state import AppState
from app.db.base import create_database_engine, create_session_factory, create_tables
from app.utils.concurrency import ConcurrencyLimiter
from app.utils.logger import configure_logging
from app.utils.stream_counter import StreamCounter
from app.websocket.manager import WebSocketManager


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """
    Creates all shared async resources once per process so requests reuse pools,
    clients, and limiters instead of recreating expensive objects repeatedly.
    """
    print("Starting up application and creating shared resources...")

    settings = get_settings()
    configure_logging(log_level=settings.log_level)

    http_client = httpx.AsyncClient(timeout=settings.http_timeout_seconds)
    db_engine = create_database_engine(database_url=settings.database_url)
    session_factory = create_session_factory(engine=db_engine)
    await create_tables(engine=db_engine)

    redis_client = None
    if not settings.redis_url.startswith("memory://"):
        redis_client = Redis.from_url(url=settings.redis_url, decode_responses=True)

    process_pool = None
    if settings.process_pool_workers is not None and settings.process_pool_workers > 0:
        process_pool = ProcessPoolExecutor(max_workers=settings.process_pool_workers)

    app.state.container = AppState(
        settings=settings,
        http_client=http_client,
        cache=AsyncCache(
            redis_client=redis_client,
            allow_in_memory_fallback=settings.enable_in_memory_cache_fallback,
            default_ttl_seconds=settings.cache_ttl_seconds,
        ),
        db_engine=db_engine,
        session_factory=session_factory,
        http_limiter=ConcurrencyLimiter(limit=settings.http_concurrency_limit),
        webhook_limiter=ConcurrencyLimiter(limit=settings.webhook_concurrency_limit),
        websocket_manager=WebSocketManager(queue_size=settings.websocket_queue_size),
        stream_counter=StreamCounter(),
        process_pool=process_pool,
    )

    yield

    print("Shutting down application and cleaning up resources...")
    await http_client.aclose()
    await app.state.container.cache.aclose()
    await db_engine.dispose()
    if process_pool is not None:
        process_pool.shutdown(wait=True, cancel_futures=True)
