"""
Write path — always executed on the PRIMARY.

The durability level maps to Postgres's per-transaction `synchronous_commit`:
  QUORUM -> 'on'    : commit waits for a synchronous standby to acknowledge
  ASYNC  -> 'local' : commit returns after the primary's local WAL flush only

We also return the primary's WAL LSN at write time, which the read path can use
to implement read-your-writes against a replica.
"""

from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db.models import Document
from app.db.replication_base import session_for, admin_session_factory
from app.schemas.consistency import (
    Durability,
    WriteResult,
    ReadNode,
    ReadResult,
    ReplicationStatusResponse,
    ReplicaStatus,
)

_SYNCHRONOUS_COMMIT = {Durability.QUORUM: "on", Durability.ASYNC: "local"}


class ConsistencyService:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def write(
        self, doc_id: str, content: str, durability: Durability
    ) -> WriteResult:
        commit_mode = _SYNCHRONOUS_COMMIT[durability]
        async with self._session.begin():
            # SET LOCAL applies to THIS transaction only. The value comes from a
            # fixed mapping above, so string-building here is safe.
            await self._session.execute(
                text(f"SET LOCAL synchronous_commit = {commit_mode}")
            )
            statement = (
                pg_insert(Document)
                .values(id=doc_id, content=content)
                .on_conflict_do_update(
                    index_elements=[Document.id],
                    set_={"content": content},
                )
                .returning(Document.updated_at)
            )
            updated_at = (await self._session.execute(statement)).scalar_one()

        # Capture the WAL position AFTER commit: the commit record has now been
        # written, so pg_current_wal_lsn() is >= our commit's LSN. (Reading it
        # inside the transaction would return a position BEFORE the commit
        # record, so a replica could reach it without yet seeing our write.)
        write_lsn = await self._session.scalar(
            text("SELECT pg_current_wal_lsn()::text")
        )
        return WriteResult(
            id=doc_id,
            content=content,
            updated_at=updated_at,
            write_lsn=write_lsn,
            durability=durability,
        )

    async def read(
        self,
        node: ReadNode,
        doc_id: str,
        wait_for_lsn: Optional[str] = None,
        wait_timeout: float = 10.0,
    ):
        """
        Read path — routes a read to the primary (strong) or a replica (eventual), and
        can enforce read-your-writes against a replica by waiting for a target WAL LSN.

        Also exposes the primary's replication overview (pg_stat_replication) so the
        demos can show which standbys are sync vs async and how far they lag.
        """
        async with session_for(node.value)() as session:
            up_to_date: Optional[bool] = None
            if wait_for_lsn is not None and node != ReadNode.PRIMARY:
                up_to_date = await self._wait_for_lsn(
                    session=session, target_lsn=wait_for_lsn, timeout=wait_timeout
                )
            document = await session.get(Document, doc_id)
            node_lsn = await self._node_lsn(session=session, node=node)
            if document is None:
                return ReadResult(
                    id=doc_id,
                    found=False,
                    served_by=node,
                    node_lsn=node_lsn,
                    up_to_date=up_to_date,
                )
            return ReadResult(
                id=document.id,
                found=True,
                content=document.content,
                updated_at=document.updated_at,
                served_by=node,
                node_lsn=node_lsn,
                up_to_date=up_to_date,
            )

    @staticmethod
    async def replication_status() -> ReplicationStatusResponse:
        """Return the primary's view of its standbys (sync state + replay lag).

        Uses the superuser (admin) connection so pg_stat_replication exposes
        state/sync_state (hidden from non-privileged roles).
        """
        async with admin_session_factory() as session:
            primary_lsn = await session.scalar(
                text("SELECT pg_current_wal_lsn()::text")
            )
            rows = (await session.execute(text("""
                        SELECT application_name, state, sync_state,
                        EXTRACT(EPOCH FROM replay_lag) AS lag
                        FROM pg_stat_replication ORDER BY application_name
                        """))).all()
        standbys = [
            ReplicaStatus(
                application_name=row.application_name or "unknown",
                state=row.state,
                sync_state=row.sync_state,
                replay_lag_seconds=float(row.lag) if row.lag is not None else None,
            )
            for row in rows
        ]
        return ReplicationStatusResponse(primary_lsn=primary_lsn, standbys=standbys)

    @staticmethod
    async def _node_lsn(session: AsyncSession, node: ReadNode) -> str:
        """
        Get the current WAL LSN of the given node.

        Returns the LSN as a string.
        """
        if node == ReadNode.PRIMARY:
            return await session.scalar(text("SELECT pg_current_wal_lsn()::text"))
        else:
            return await session.scalar(text("SELECT pg_last_wal_replay_lsn()::text"))

    @staticmethod
    async def _wait_for_lsn(
        session: AsyncSession, target_lsn: str, timeout: float
    ) -> bool:
        """
        Wait for the replica to catch up to the given WAL LSN.

        Returns True if the replica is up-to-date, False if the timeout was reached.
        """
        # Use a simple polling loop with a timeout. In a real system, you might want
        # to use LISTEN/NOTIFY or another mechanism to avoid busy-waiting.
        import asyncio
        import time

        deadline = time.monotonic() + timeout
        while True:
            reached = await session.scalar(
                text(
                    "SELECT pg_last_wal_replay_lsn() >= CAST(:target_lsn as text)::pg_lsn"
                ),
                {"target_lsn": target_lsn},
            )
            if reached:
                return True
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.05)
