from datetime import datetime

from pydantic import BaseModel, Field


class CreateOrderRequest(BaseModel):
    item: str
    quantity: int = Field(gt=0)


class OrderResponse(BaseModel):
    id: str
    item: str
    quantity: int
    created_at: datetime


class SimulateResponse(BaseModel):
    """Result of a simulated-work request (used to shape the latency histogram)."""

    slept_ms: int
    message: str


class StatsResponse(BaseModel):
    """
    Latency + traffic stats computed in Python from the in-process sample buffer.

    This mirrors, in plain code, what Prometheus computes from its histogram:
    percentiles, throughput, and error rate over the recent window.
    """

    sample_count: int
    throughput_rps: float          # requests per second over the observed window
    error_rate: float              # fraction of samples with a 5xx status (0..1)
    p50_ms: float
    p95_ms: float
    p99_ms: float