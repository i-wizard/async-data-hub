import asyncio
from typing import Dict

from fastapi import APIRouter, Query

router = APIRouter(prefix="/mock-sources")


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

    return await _mock_source_payload(source_name="weather", query=q, delay_seconds=0.1)


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

    return await _mock_source_payload(source_name="news", query=q, delay_seconds=0.2)


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
