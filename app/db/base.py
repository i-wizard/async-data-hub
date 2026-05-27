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

    async_sessionmaker(engine, expire_on_commit=False)
    By default this is True, which means: on commit(), every ORM object in the session is marked stale ("expired").
    The next time you touch any attribute, SQLAlchemy issues a fresh SELECT to reload it.


    # expire_on_commit=True (default)
    await session.commit()
    print(record.id)   # triggers a reload SELECT to refresh the expired object

    # expire_on_commit=False
    await session.commit()
    print(record.id)   # no DB hit — the value stays cached on the object

    This matters intensely in async.
    That implicit reload needs to emit SQL, but attribute access (record.id) is plain synchronous Python — you can't await it.
    So in async SQLAlchemy, touching an expired attribute after commit throws the infamous MissingGreenlet / "greenlet_spawn has not been called" error.
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
