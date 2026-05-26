from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from app.config.dependencies import aggregation_service
from app.schemas.aggregation import AggregationResponse
from app.services.aggregation_service import AggregationService

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
