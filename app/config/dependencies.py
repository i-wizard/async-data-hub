from typing import AsyncIterator

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import HTTPConnection

from app.cache.redis_cache import AsyncCache
from app.config.settings import Settings, get_settings
from app.core.state import AppState
from app.services.aggregation_service import AggregationService
from app.services.consistency import ConsistencyService
from app.services.cpu_service import CpuAnalysisService
from app.services.db_concurrency_service import DBConcurrencyService
from app.services.health_service import HealthService
from app.services.inventory_service import InventoryService
from app.services.media_service import MediaService
from app.services.payment_service import PaymentService
from app.services.webhook_service import WebhookService
from app.utils.http import SharedHttpClient
from app.websocket.manager import WebSocketManager


def app_settings() -> Settings:
    """
    Exposes cached settings through dependency injection so route handlers remain
    decoupled from direct environment access.
    """

    return get_settings()


def app_state(connection: HTTPConnection) -> AppState:
    """
    Returns the lifespan-created resource container so services can reuse shared
    async infrastructure instead of constructing their own clients or pools.
    """

    return connection.app.state.container


async def db_session(
    state: AppState = Depends(app_state),
) -> AsyncIterator[AsyncSession]:
    """
    Provides one async database session per request so transactions stay scoped
    and predictable even when services perform concurrent I/O elsewhere.
    """

    async with state.session_factory() as session:
        yield session


def cache_client(state: AppState = Depends(app_state)) -> AsyncCache:
    """
    Supplies the shared cache wrapper so request code uses one consistent cache
    policy regardless of whether Redis or in-memory fallback is active.
    """

    return state.cache


def http_client(state: AppState = Depends(app_state)) -> SharedHttpClient:
    """
    Wraps the shared AsyncClient with limiter-aware helper logic so outbound
    calls consistently honor timeouts and concurrency limits.
    """

    return SharedHttpClient(client=state.http_client, limiter=state.http_limiter)


def websocket_manager(state: AppState = Depends(app_state)) -> WebSocketManager:
    """
    Exposes the connection manager so services can publish events without
    owning transport-specific lifecycle details.
    """

    return state.websocket_manager


def health_service(
    state: AppState = Depends(app_state),
    cache: AsyncCache = Depends(cache_client),
) -> HealthService:
    """
    Assembles the health service from shared resources so readiness checks prove
    the same dependencies used by real requests.
    """

    return HealthService(state=state, cache=cache)


def aggregation_service(
    state: AppState = Depends(app_state),
    session: AsyncSession = Depends(db_session),
    cache: AsyncCache = Depends(cache_client),
    client: SharedHttpClient = Depends(http_client),
    manager: WebSocketManager = Depends(websocket_manager),
) -> AggregationService:
    """
    Builds the aggregation service with all shared async collaborators so route
    handlers stay thin and the workflow remains testable in isolation.
    """

    return AggregationService(
        state=state,
        session=session,
        cache=cache,
        http_client=client,
        websocket_manager=manager,
    )


def webhook_service(
    state: AppState = Depends(app_state),
    session: AsyncSession = Depends(db_session),
    client: SharedHttpClient = Depends(http_client),
    manager: WebSocketManager = Depends(websocket_manager),
) -> WebhookService:
    """
    Provides the webhook service through dependency injection so fan-out logic
    can reuse the shared limiter, HTTP client, and database session.
    """

    return WebhookService(
        state=state,
        session=session,
        http_client=client,
        websocket_manager=manager,
    )


def cpu_analysis_service(state: AppState = Depends(app_state)) -> CpuAnalysisService:
    """
    Exposes CPU analysis helpers through a service layer so route handlers can
    compare execution strategies without embedding process management logic.
    """

    return CpuAnalysisService(state=state)


def media_service(state: AppState = Depends(app_state)) -> MediaService:
    """
    Builds the media service from shared settings so the streaming endpoints
    can resolve files and stream chunks without touching the filesystem directly.
    """

    return MediaService(state=state)


def payment_service(
    state: AppState = Depends(app_state),
    session: AsyncSession = Depends(db_session),
    cache: AsyncCache = Depends(cache_client),
) -> PaymentService:
    """
    Provides the payment service through dependency injection so payment logic
    can reuse the shared database session and cache.
    """

    return PaymentService(session=session, cache=cache)


def inventory_service(
    session: AsyncSession = Depends(db_session),
    cache: AsyncCache = Depends(cache_client),
) -> InventoryService:
    """
    Provides the inventory service through dependency injection so inventory logic
    can reuse the shared database session and cache.
    """

    return InventoryService(session=session, cache=cache)


def consistency_service(
    session: AsyncSession = Depends(db_session),
) -> ConsistencyService:
    """
    Provides the consistency service through dependency injection so consistency logic
    can reuse the shared database session and cache.
    """

    return ConsistencyService(session=session)


def db_concurrency_service(session: AsyncSession = Depends(db_session)):
    return DBConcurrencyService(session=session)
