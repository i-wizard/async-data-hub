import asyncio
import time

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from app.config.dependencies import aggregation_service
from app.schemas.aggregation import AggregationResponse, SleepMode, SleepResult
from app.services.aggregation_service import AggregationService
from app.utils.logger import CustomLogger

router = APIRouter()


@router.get(
    "/aggregate",
    response_model=AggregationResponse,
    response_description="Aggregate multiple async upstream sources",
)
async def aggregate(
    q: str = Query(..., min_length=1),
    use_cache: bool = Query(True),
    _service: AggregationService = Depends(aggregation_service),
) -> AggregationResponse:
    """
    Returns one full aggregate response so users can compare the standard batch
    experience against the streaming variant built on the same service.
    """

    return await _service.aggregate(query=q, use_cache=use_cache)


@router.get(
    "/aggregate/stream",
    response_model=None,
    response_class=StreamingResponse,
    response_description="Stream aggregate results over server-sent events",
)
async def aggregate_stream(
    q: str = Query(..., min_length=1),
    _service: AggregationService = Depends(aggregation_service),
) -> StreamingResponse:
    """
    Streams partial aggregate results over SSE because this project is meant to
    make async progress and cancellation behavior visible to the learner.
    """

    return StreamingResponse(
        _service.stream_aggregate(query=q),
        media_type="text/event-stream",
    )


@router.get(
    "/sleep/async",
    response_model=SleepResult,
    response_description="Sleep without blocking the event loop and report elapsed time",
)
async def sleep_async(seconds: float = Query(..., ge=0)) -> SleepResult:
    """
    Sleeps asynchronously for the requested seconds so the event loop stays free
    to serve other requests, then reports how long the task actually took.
    Use this scripts/load_test.py to compare how the async and sync variants handle load differently.
    """
    # CustomLogger.info("Starting async sleep for {seconds} seconds".format(seconds=seconds))
    started_at = time.perf_counter()
    await asyncio.sleep(seconds)
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)

    return SleepResult(mode=SleepMode.ASYNC, requested_seconds=seconds, elapsed_ms=elapsed_ms)


@router.get(
    "/sleep/sync",
    response_model=SleepResult,
    response_description="Sleep on the worker thread and report elapsed time",
)
def sleep_sync(seconds: float = Query(..., ge=0)) -> SleepResult:
    """
    Sleeps with a blocking call to contrast with the async variant, then reports
    how long the task actually took so the handling difference is visible.
    """
    # CustomLogger.info(f"Starting sync sleep for {seconds} seconds".format(seconds=seconds))
    started_at = time.perf_counter()
    time.sleep(seconds)
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)

    return SleepResult(mode=SleepMode.SYNC, requested_seconds=seconds, elapsed_ms=elapsed_ms)
