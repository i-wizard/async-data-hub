import asyncio
from typing import Dict, Optional

from fastapi import APIRouter, Header, Query, Response, status

router = APIRouter(prefix="/mock-sources")
MOCK_MEDIA_CONTENT = bytes(range(256)) * 16


async def _mock_source_payload(source_name: str, query: str, delay_seconds: float) -> Dict:
    """
    Simulates an upstream HTTP service so aggregation can demonstrate real async
    fan-out locally without depending on third-party APIs or network access.
    """

    await asyncio.sleep(delay_seconds)
    return {
        "source": source_name,
        "query": query,
        "summary": "{source_name} data for {query}".format(source_name=source_name, query=query),
    }


@router.get(
    "/weather",
    response_model=Dict,
    response_description="Mock weather upstream for async aggregation demos",
)
async def weather_source(q: str = Query(..., min_length=1)) -> Dict:
    """
    Returns delayed weather data because the aggregate endpoint needs multiple
    differently timed sources to make concurrency visible.
    """

    return await _mock_source_payload(source_name="weather", query=q, delay_seconds=1)


@router.get(
    "/news",
    response_model=Dict,
    response_description="Mock news upstream for async aggregation demos",
)
async def news_source(q: str = Query(..., min_length=1)) -> Dict:
    """
    Returns delayed news data so one source can finish later than another and
    show why streaming partial results is useful.
    """

    return await _mock_source_payload(source_name="news", query=q, delay_seconds=2)


@router.get(
    "/finance",
    response_model=Dict,
    response_description="Mock finance upstream for async aggregation demos",
)
async def finance_source(q: str = Query(..., min_length=1)) -> Dict:
    """
    Returns delayed finance data to round out the local multi-source aggregation
    demo with staggered completion times.
    """

    return await _mock_source_payload(source_name="finance", query=q, delay_seconds=0.15)


@router.get(
    "/media.mp4",
    response_description="Mock range-aware media upstream for remote streaming demos",
)
async def mock_media_source(
    range_header: Optional[str] = Header(default=None, alias="Range"),
) -> Response:
    """
    Provides a deterministic byte-range-capable upstream so remote media proxy
    behavior can be tested locally without relying on third-party media URLs.
    """

    headers = {
        "Content-Type": "video/mp4",
        "Accept-Ranges": "bytes",
    }
    total = len(MOCK_MEDIA_CONTENT)

    if range_header is None:
        headers["Content-Length"] = str(total)
        return Response(content=MOCK_MEDIA_CONTENT, headers=headers, media_type="video/mp4")

    prefix = "bytes="
    if not range_header.startswith(prefix):
        headers["Content-Range"] = "bytes */{total}".format(total=total)
        return Response(
            content=b"",
            status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            headers=headers,
        )

    start_text, _, end_text = range_header[len(prefix):].partition("-")
    try:
        start = int(start_text)
        end = int(end_text) if end_text else total - 1
    except ValueError:
        headers["Content-Range"] = "bytes */{total}".format(total=total)
        return Response(
            content=b"",
            status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            headers=headers,
        )

    if start < 0 or start >= total or end < start:
        headers["Content-Range"] = "bytes */{total}".format(total=total)
        return Response(
            content=b"",
            status_code=status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            headers=headers,
        )

    end = min(end, total - 1)
    headers["Content-Range"] = "bytes {start}-{end}/{total}".format(
        start=start,
        end=end,
        total=total,
    )
    headers["Content-Length"] = str(end - start + 1)
    return Response(
        content=MOCK_MEDIA_CONTENT[start:end + 1],
        status_code=status.HTTP_206_PARTIAL_CONTENT,
        headers=headers,
        media_type="video/mp4",
    )
