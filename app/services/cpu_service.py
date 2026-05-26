import asyncio
from typing import Optional

from app.core.state import AppState
from app.schemas.analysis import AnalysisStrategy, HeavyAnalysisResponse


def nth_fibonacci(number: int) -> int:
    """
    Uses intentionally expensive recursive CPU work so the endpoint can make
    blocking behavior obvious without needing domain-specific algorithms.
    """

    if number <= 1:
        return number
    return nth_fibonacci(number=number - 1) + nth_fibonacci(number=number - 2)


class CpuAnalysisService:
    """
    Compares CPU execution strategies so users can see why async I/O patterns do
    not automatically solve CPU-bound workloads.
    """

    def __init__(self, state: AppState):
        """
        Receives the process-wide runtime container because process-pool access
        belongs to application scope rather than route-local setup.
        """

        self._state = state

    async def analyze(self, number: int, strategy: AnalysisStrategy) -> HeavyAnalysisResponse:
        """
        Executes the same workload through different strategies so the endpoint
        isolates the impact of blocking vs offloading choices.
        """

        if strategy == AnalysisStrategy.BLOCKING:
            result = nth_fibonacci(number=number)
        elif strategy == AnalysisStrategy.THREADPOOL:
            result = await asyncio.to_thread(nth_fibonacci, number)
        else:
            loop = asyncio.get_running_loop()
            process_pool = self._state.process_pool
            if process_pool is None:
                result = await loop.run_in_executor(None, nth_fibonacci, number)
            else:
                result = await loop.run_in_executor(process_pool, nth_fibonacci, number)

        return HeavyAnalysisResponse(strategy=strategy, number=number, result=result)
