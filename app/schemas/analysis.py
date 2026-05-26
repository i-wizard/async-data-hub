from enum import Enum

from pydantic import BaseModel, Field


class AnalysisStrategy(str, Enum):
    """
    Makes CPU execution modes explicit so callers can compare async misuse
    against proper offloading without relying on magic strings.
    """

    BLOCKING = "blocking"
    THREADPOOL = "threadpool"
    PROCESSPOOL = "processpool"


class HeavyAnalysisRequest(BaseModel):
    """
    Defines the CPU analysis workload so the endpoint can demonstrate strategy
    differences with a tunable but bounded input size.
    """

    number: int = Field(default=28, ge=20, le=35)
    strategy: AnalysisStrategy


class HeavyAnalysisResponse(BaseModel):
    """
    Returns the chosen strategy and result so users can compare endpoint
    behavior while keeping the computation itself simple.
    """

    strategy: AnalysisStrategy
    number: int
    result: int
