from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class AggregationSourceResult(BaseModel):
    """
    Represents one upstream result so partial success is first-class instead of
    forcing the whole aggregation into a single success/failure flag.
    """

    source: str
    status: str
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    duration_ms: Optional[int] = None


class AggregationResponse(BaseModel):
    """
    Returns aggregate metadata plus per-source results so callers can observe
    how concurrent fan-out behaved during the request.
    """

    request_id: int
    query: str
    status: str
    cached: bool
    results: List[AggregationSourceResult]


class AggregationStreamEvent(BaseModel):
    """
    Defines the SSE payload shape so streaming and non-streaming aggregation use
    the same vocabulary for partial results.
    """

    request_id: Optional[int] = None
    event: str
    source: Optional[str] = None
    query: str
    payload: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
