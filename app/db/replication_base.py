"""
Async SQLAlchemy setup for a PRIMARY + REPLICAS topology.

One engine per node. Writes go to the primary; reads can be routed to the
primary (strong) or a replica (eventual). Tables are created on the primary
only — the replicas receive the DDL through streaming replication.
"""

import asyncio
from typing import Dict

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
)

from app.db.base import Base
from app.utils.logger import CustomLogger
from scripts.payment.load_customers import settings

PRIMARY = "primary"
REPLICA1 = "replica1"
REPLICA2 = "replica2"
REPLICAS = (REPLICA1, REPLICA2)


engines: Dict[str, AsyncEngine] = {
    PRIMARY: create_async_engine(settings.database_url),
    REPLICA1: create_async_engine(settings.database_replica_url),
    REPLICA2: create_async_engine(settings.database_replica_url2),
}

session_factories: Dict[str, async_sessionmaker[AsyncSession]] = {
    node: async_sessionmaker(bind=engine, expire_on_commit=False)
    for node, engine in engines.items()
}

# Superuser connection to the primary, used only for the replication-status query.
admin_engine: AsyncEngine = create_async_engine(settings.database_admin_url)
admin_session_factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=admin_engine, expire_on_commit=False
)


def session_for(node: str) -> async_sessionmaker[AsyncSession]:
    return session_factories[node]


async def init_models() -> None:
    """Create tables on the PRIMARY; replicas get them via replication."""
    async with engines[PRIMARY].begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def dispose_engines() -> None:
    """Dispose all engines to clean up connections."""
    for engine in engines.values():
        await engine.dispose()
    await admin_engine.dispose()


async def init_with_retry(attempts: int = 30, delay: float = 2.0) -> None:
    """Create tables on the primary, retrying while the cluster settles."""
    for attempt in range(1, attempts + 1):
        try:
            await init_models()
            CustomLogger.info(f"Primary schema ready {attempt}")
            return
        except Exception as exc:  # noqa: BLE001 - startup bootstrap, log and retry
            CustomLogger.warning(f"init_models attempt: {attempt} failed: {exc}")
            await asyncio.sleep(delay)
    raise RuntimeError("could not initialize the primary schema")
