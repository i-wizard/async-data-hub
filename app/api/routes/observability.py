import asyncio

from fastapi import APIRouter, Depends, Query

from app.config.dependencies import (
    observability_stat_service,
    observability_order_service,
)
from app.schemas.observability import (
    StatsResponse,
    SimulateResponse,
    OrderResponse,
    CreateOrderRequest,
)
from app.services.observability import (
    ObservabilityStatsService,
    ObservabilityOrderService,
)

router = APIRouter()

_MAX_SLEEP_MS = 10_000


class SimulatedFailure(Exception):
    """Raised by /simulate/error to produce a deterministic 500 for the demo."""


@router.get(
    "/stats",
    response_model=StatsResponse,
    response_description="Latency percentiles, throughput and error rate (computed in Python).",
)
async def get_stats(
    _service: ObservabilityStatsService = Depends(observability_stat_service),
):
    return _service.compute()


@router.get("/simulate/work", response_model=SimulateResponse)
async def simulate_work(
    ms: int = Query(
        50, ge=0, le=_MAX_SLEEP_MS, description="Milliseconds to sleep (0-10000)"
    ),
    _service: ObservabilityStatsService = Depends(observability_stat_service),
):
    await asyncio.sleep(ms / 1000)
    return SimulateResponse(slept_ms=ms, message="ok")


@router.get("/simulate/error", response_model=SimulateResponse)
async def simulate_error(
    _service: ObservabilityStatsService = Depends(observability_stat_service),
):
    raise SimulatedFailure("Simulated failure for demo purposes")


@router.post("/orders", response_model=OrderResponse, status_code=201)
async def create_order(
    data: CreateOrderRequest,
    _service: ObservabilityOrderService = Depends(observability_order_service),
):
    return _service.create(item=data.item, quantity=data.quantity)


@router.get("/orders/{order_id}", response_model=OrderResponse)
async def get_order(
    order_id: str,
    _service: ObservabilityOrderService = Depends(observability_order_service),
):
    return _service.get(order_id=order_id)
