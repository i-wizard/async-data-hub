"""
Job-queue controllers (thin) — HTTP only; all locking logic is in the service.
"""

from typing import List

from fastapi import APIRouter, Depends, Query, status

from app.config.dependencies import db_lock_service
from app.schemas.db_concurrency import (
    EnqueuedJobResponse,
    EnqueueRequest,
    ClaimResponse,
    ClaimStrategy,
)
from app.services.db_lock_service import DBLockService

router = APIRouter(tags=["queue"])


@router.post(
    "/jobs",
    response_model=EnqueuedJobResponse,
    status_code=status.HTTP_201_CREATED,
    response_description="Enqueue a new job.",
)
async def enqueue_job(
    data: EnqueueRequest,
    _service: DBLockService = Depends(db_lock_service),
) -> EnqueuedJobResponse:
    """Add a job to the queue."""
    return await _service.enqueue(payload=data.payload)


@router.post(
    "/jobs/claim",
    response_model=ClaimResponse,
    response_description="Claim the oldest queued job with the chosen lock strategy.",
)
async def claim_job(
    worker: str = Query(
        ..., min_length=1, description="Identifier of the claiming worker."
    ),
    strategy: ClaimStrategy = Query(
        ClaimStrategy.SKIP_LOCKED, description="skip_locked | blocking | nowait"
    ),
    _service: DBLockService = Depends(db_lock_service),
) -> ClaimResponse:
    """Claim the next available job for a worker."""
    return await _service.claim(worker=worker, strategy=strategy)


@router.post(
    "/jobs/{job_id}/complete",
    response_model=EnqueuedJobResponse,
    response_description="Mark a claimed job as done.",
)
async def complete_job(
    job_id: str,
    _service: DBLockService = Depends(db_lock_service),
) -> EnqueuedJobResponse:
    """Mark a job DONE."""
    return await _service.complete(job_id=job_id)


@router.get(
    "/jobs/{job_id}",
    response_model=EnqueuedJobResponse,
    response_description="Fetch a job by id.",
)
async def get_job(
    job_id: str,
    _service: DBLockService = Depends(db_lock_service),
) -> EnqueuedJobResponse:
    """Return a job, or 404 if it does not exist."""
    return await _service.get(job_id=job_id)


@router.get(
    "/jobs",
    response_model=List[EnqueuedJobResponse],
    response_description="List all jobs in FIFO order.",
)
async def list_jobs(
    _service: DBLockService = Depends(db_lock_service),
) -> List[EnqueuedJobResponse]:
    """Return every job in queue order (for inspection)."""
    return await _service.list_jobs()
