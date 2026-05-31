from fastapi import APIRouter, Depends

from app.config.dependencies import cpu_analysis_service
from app.schemas.analysis import HeavyAnalysisRequest, HeavyAnalysisResponse
from app.services.cpu_service import CpuAnalysisService

router = APIRouter()


@router.post(
    "/heavy-analysis",
    response_model=HeavyAnalysisResponse,
    response_description="Compare CPU execution strategies in an async API",
)
async def heavy_analysis(
    data: HeavyAnalysisRequest,
    _service: CpuAnalysisService = Depends(cpu_analysis_service),
) -> HeavyAnalysisResponse:
    """
    Compares blocking, threadpool, and process-style execution because async
    systems engineering includes knowing when not to rely on async alone.
    """

    return await _service.analyze(number=data.number, strategy=data.strategy)


@router.post(
    "/heavy-analysis/sync",
    response_model=HeavyAnalysisResponse,
    response_description="Compare CPU execution strategies in an sync API",
)
def heavy_analysis_sync(
    data: HeavyAnalysisRequest,
    _service: CpuAnalysisService = Depends(cpu_analysis_service),
) -> HeavyAnalysisResponse:
    """
    Provides the same CPU execution comparison as the async endpoint but in a
    sync context to show that blocking behavior is not unique to async APIs.
    """

    return _service.analyze_sync(number=data.number)
