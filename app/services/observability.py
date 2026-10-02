import math
import uuid
from datetime import datetime, timezone
from typing import Sequence, Tuple, List, Deque, Dict

from app.schemas.observability import StatsResponse, OrderResponse


def percentiles(values: Sequence[float], q: float) -> float:
    """
    Return the q-th percentile (q in 0..100) using the nearest-rank method.

    Nearest-rank: sort, take the value at rank ceil(q/100 * n). It is simple and
    exact for a given sample set — no interpolation — which is why P99 of 1..100
    is exactly 99. (Prometheus interpolates instead; see histogram_quantile.)
    e.g values = [1, 2, 3, 4, 5], q = 50 -> 3.0
    """
    if not values:
        raise ValueError("percentiles() requires at least one value")
    ordered = sorted(values)
    if q <= 0:
        return float(ordered[0])
    if q >= 100:
        return float(ordered[-1])
    rank = math.ceil(q / 100 * len(values))
    return float(ordered[max(1, rank) - 1])


def error_rate(total: int, errors: int) -> float:
    """Fraction of requests that errored (0..1); 0 when there is no traffic."""
    if total <= 0:
        return 0.0
    return errors / total

def throughput(count: int, seconds: float) -> float:
    """Requests per second: count over a window; 0 when the window is empty."""
    if seconds <= 0:
        return 0.0
    return count / seconds

def histogram_quantile(buckets: List[Tuple[float, float]], q: float) -> float:
    """
    Estimate the q-th percentile from cumulative histogram buckets, the way
    Prometheus's histogram_quantile() does.

    `buckets` is [(upper_bound, cumulative_count), ...] sorted ascending by bound,
    with the final bound being math.inf. We find the bucket the rank falls in and
    LINEARLY INTERPOLATE within its [lower, upper] range. This is why a histogram
    percentile is only as precise as the bucket boundaries.
    """
    if not buckets:
        raise ValueError("histogram_quantile() requires at least one bucket")
    total = buckets[-1][1]
    if total <= 0:
        return 0.0

    rank = q / 100 * total
    lower_bound = 0.0
    lower_count = 0.0
    for upper_bound, cumulative in buckets:
        if cumulative >= rank:
            if math.isinf(upper_bound):
                return lower_bound  # the +inf bucket has no finite upper edge
            in_bucket = cumulative - lower_count
            if in_bucket <= 0:
                return upper_bound
            fraction = (rank - lower_count) / in_bucket
            return lower_bound + fraction * (upper_bound - lower_bound)
        lower_bound, lower_count = upper_bound, cumulative
    return buckets[-1][0]

Sample = Tuple[float, int, float]  # (latency_seconds, status_code, completed_at_epoch_seconds)

class ObservabilityStatsService:
    def __init__(self, samples: Deque[Sample]) -> None:
        self._samples = samples

    def compute(self) -> StatsResponse:
        """Compute count, throughput, error rate and P50/P95/P99 over the buffer."""

        snapshot = list(self._samples) # Take a snapshot of the current samples to avoid mutation during computation
        count = len(snapshot)
        if not count:
            return StatsResponse(
                sample_count=0,
                throughput_rps=0.0,
                error_rate=0.0,
                p50_ms=0.0,
                p95_ms=0.0,
                p99_ms=0.0,
            )
        latencies_ms = [sample[0] for sample in snapshot]
        errors = sum(1 for sample in snapshot if sample[1] >= 500)
        timestamps = [sample[2] for sample in snapshot]
        window_seconds = max(timestamps) - min(timestamps)

        return StatsResponse(
            sample_count=count,
            throughput_rps=round(throughput(count=count, seconds=window_seconds), 3),
            error_rate=round(error_rate(total=count, errors=errors), 4),
            p50_ms=round(percentiles(latencies_ms, 50), 3),
            p95_ms=round(percentiles(latencies_ms, 95), 3),
            p99_ms=round(percentiles(latencies_ms, 99), 3)
        )


class OrderNotFound(Exception):
    """Raised when an order id does not exist (-> 404)."""


class ObservabilityOrderService:
    def __init__(self, store: Dict[str, dict]) -> None:
        self._store = store

    def create(self, item: str, quantity: int) -> OrderResponse:
        order = {
            "id":f"ord_{uuid.uuid4().hex[:12]}",
            "item": item,
            "quantity": quantity,
            "created_at": datetime.now(timezone.utc)
        }
        self._store[order["id"]] = order
        return OrderResponse(**order)

    def get(self, order_id: str) -> OrderResponse:
        order = self._store.get(order_id)
        if order is None:
            raise OrderNotFound(order_id)
        return OrderResponse(**order)