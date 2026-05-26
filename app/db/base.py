from typing import AsyncIterator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """
    Defines the SQLAlchemy declarative base so all durable async models share a
    common metadata registry for startup table creation.
    """


def create_database_engine(database_url: str) -> AsyncEngine:
    """
    Creates one async engine per process because connection pooling belongs at
    application scope, not inside individual requests.
    """

    return create_async_engine(database_url, future=True)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """
    Builds a reusable async session factory so request-scoped sessions can be
    created cheaply while sharing the process-wide connection pool.
    """

    return async_sessionmaker(engine, expire_on_commit=False)


async def create_tables(engine: AsyncEngine) -> None:
    """
    Creates tables during startup for this learning project so the first run is
    frictionless and the focus stays on async behavior rather than migrations.
    """

    from app.db import models  # noqa: F401

    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
