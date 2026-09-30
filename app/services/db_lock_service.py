from typing import Sequence, List

from sqlalchemy import select
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import DbQueueJob
from app.schemas.db_concurrency import EnqueuedJobResponse, EnqueuedJobStatus, ClaimStrategy, ClaimResponse

SQLSTATE_LOCK_NOT_AVAILABLE = "55P03"  # Postgres error code for "lock not available"
SQLSTATE_DEADLOCK_DETECTED = "40P01"  # Postgres error code for "deadlock detected"

SHARE = 'share'
EXCLUSIVE = 'exclusive'

class DBLockUnavailable(Exception):
    """Raised when a NOWAIT claim hit a row locked by another worker (-> 409)."""
    ...



class DBLockService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session


    async def enqueue(self, payload: str) -> EnqueuedJobResponse:
        """Add a new QUEUED job and return it (with its assigned FIFO position)."""
        async with self._session.begin():
            job = DbQueueJob(payload=payload, status=EnqueuedJobStatus.QUEUED.value)
            self._session.add(job)
            await self._session.flush()
            return EnqueuedJobResponse.model_validate(job)

    async def claim(self, worker: str, strategy: str) -> ClaimResponse:
        """Claim the oldest QUEUED job using the chosen locking strategy."""
        statement = (
            select(DbQueueJob)
            .where(DbQueueJob.status == EnqueuedJobStatus.QUEUED.value)
            .order_by(DbQueueJob.position)
            .limit(1)
        )
        if strategy == ClaimStrategy.SKIP_LOCKED:
            statement = statement.with_for_update(skip_locked=True)
        elif strategy == ClaimStrategy.BLOCKING:
            statement = statement.with_for_update()
        elif strategy == ClaimStrategy.NOWAIT:
            statement = statement.with_for_update(nowait=True)

        try:
            async with self._session.begin():
                job = (await self._session.execute(statement)).scalar_one_or_none()
                if job is None:
                    return ClaimResponse(claimed=False)
                job.status = EnqueuedJobStatus.RUNNING.value
                job.locked_by = worker
                await self._session.flush()
                return ClaimResponse(claimed=True, job=EnqueuedJobResponse.model_validate(job))
        except DBAPIError as exc:
            sqlstate = getattr(getattr(exc, "orig", None), "sqlstate", None)
            if self._is_lock_not_available(sqlstate):
                raise DBLockUnavailable(worker) from exc
            raise

    async def complete(self, job_id: str) -> EnqueuedJobResponse:
        """Mark a RUNNING job as DONE."""
        async with self._session.begin():
            job = await self._session.get(DbQueueJob, job_id)
            if job is None:
                raise ValueError(f"Job {job_id} not found")
            job.status = EnqueuedJobStatus.DONE.value
            await self._session.flush()
            return EnqueuedJobResponse.model_validate(job)

    async def get_job(self, job_id: str) -> EnqueuedJobResponse:
        """Return a job by id, or raise ValueError if not found."""
        job = await self._session.get(DbQueueJob, job_id)
        if job is None:
            raise ValueError(f"Job {job_id} not found")
        return EnqueuedJobResponse.model_validate(job)

    async def list_jobs(self, ) -> List[EnqueuedJobResponse]:
        """Return all jobs in FIFO order (for inspection)."""
        result = await self._session.execute(select(DbQueueJob).order_by(DbQueueJob.position))
        return [EnqueuedJobResponse.model_validate(job) for job in result.scalars().all()]

    @staticmethod
    def _is_lock_not_available(sqlstate: str) -> bool:
        """True if a NOWAIT lock attempt failed because the row was already locked."""
        return sqlstate == SQLSTATE_LOCK_NOT_AVAILABLE

    @staticmethod
    def _is_deadlock(sqlstate: str) -> bool:
        """True if Postgres aborted this transaction to resolve a deadlock."""
        return sqlstate == SQLSTATE_DEADLOCK_DETECTED

    @staticmethod
    def _lock_compatible(mode_a: str, mode_b: str) -> bool:
        """
        Can two lock requests on the SAME row be held at once?

        Only SHARE + SHARE is compatible; anything involving EXCLUSIVE conflicts.
        """
        return mode_a == mode_b == SHARE

    @staticmethod
    def _order_locks(resource_ids: Sequence[str]) -> List[str]:
        """
        Return resource ids in a canonical (sorted) order.

        Acquiring locks in this fixed order in EVERY transaction is the standard way
        to prevent deadlocks: two transactions can no longer lock the same pair in
        opposite orders, so no wait-for cycle can form.
        """
        return sorted(resource_ids)
