"""
Load-tests the sleep endpoints by firing many requests at once so the async vs
sync handling difference becomes visible under simulated user load.

Because `requests` is blocking, each simulated user runs on its own thread via a
ThreadPoolExecutor — that is what lets the calls actually overlap in time.

Run from a notebook (install requests first: `!pip install requests`):

    from scripts.load_test import load_test

    load_test(path="/api/v1/sleep/async", users=50, seconds=2)
    load_test(path="/api/v1/sleep/sync", users=50, seconds=2)

The same function drives both endpoints — only `path` changes.
"""

import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from statistics import mean, median
from typing import List, Optional

import requests

DEFAULT_BASE_URL = "http://localhost:8000"


@dataclass
class RequestResult:
    """
    Captures one simulated user's outcome so per-request timing and any failure
    can be inspected individually after the run.
    """

    user_id: int
    status_code: Optional[int]
    client_elapsed_ms: int
    server_elapsed_ms: Optional[int]
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None and self.status_code == 200


@dataclass
class LoadTestResult:
    """
    Aggregates a whole run so the summary can be printed and the raw per-user
    results remain available for further analysis in the notebook.
    """

    path: str
    users: int
    seconds: float
    wall_clock_ms: int
    results: List[RequestResult] = field(default_factory=list)

    @property
    def successes(self) -> List[RequestResult]:
        return [r for r in self.results if r.ok]

    @property
    def failures(self) -> List[RequestResult]:
        return [r for r in self.results if not r.ok]


def _call_once(user_id: int, url: str, params: dict, timeout: float) -> RequestResult:
    """
    Issues a single blocking GET and records how long the client waited, keeping
    one slow or failed user from aborting the rest of the run.
    """

    started_at = time.perf_counter()
    try:
        response = requests.get(url, params=params, timeout=timeout)
        client_elapsed_ms = int((time.perf_counter() - started_at) * 1000)

        server_elapsed_ms = None
        error = None
        if response.status_code == 200:
            server_elapsed_ms = response.json().get("elapsed_ms")
        else:
            error = "HTTP {code}: {body}".format(code=response.status_code, body=response.text[:200])

        return RequestResult(
            user_id=user_id,
            status_code=response.status_code,
            client_elapsed_ms=client_elapsed_ms,
            server_elapsed_ms=server_elapsed_ms,
            error=error,
        )
    except requests.RequestException as exc:
        client_elapsed_ms = int((time.perf_counter() - started_at) * 1000)
        return RequestResult(
            user_id=user_id,
            status_code=None,
            client_elapsed_ms=client_elapsed_ms,
            server_elapsed_ms=None,
            error=str(exc),
        )


def _percentile(values: List[int], pct: float) -> int:
    """Returns the nearest-rank percentile so tail latency is easy to read in the summary."""

    if not values:
        return 0
    ordered = sorted(values)
    rank = max(0, min(len(ordered) - 1, int(round(pct / 100 * len(ordered))) - 1))
    return ordered[rank]


def load_test(
    path: str,
    users: int,
    seconds: float,
    base_url: str = DEFAULT_BASE_URL,
    timeout: Optional[float] = None,
) -> LoadTestResult:
    """
    Fires `users` concurrent requests at `path` (each asking the server to sleep
    `seconds`) and prints a timing summary so the async and sync endpoints can be
    compared on identical load. Returns the full result for further analysis.

    The wall-clock total is the headline metric: the async endpoint should stay
    close to `seconds` regardless of user count, while the sync endpoint is capped
    by the server's worker-thread pool and climbs once users exceed it.
    """

    url = base_url.rstrip("/") + path
    params = {"seconds": seconds}
    # Give each request well over the sleep duration so a queued sync request
    # waiting on a busy thread pool is not mistaken for a failure.
    effective_timeout = timeout if timeout is not None else seconds + 60

    wall_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=users) as executor:
        futures = [
            executor.submit(_call_once, user_id, url, params, effective_timeout)
            for user_id in range(users)
        ]
        results = [future.result() for future in futures]
    wall_clock_ms = int((time.perf_counter() - wall_start) * 1000)

    run = LoadTestResult(
        path=path,
        users=users,
        seconds=seconds,
        wall_clock_ms=wall_clock_ms,
        results=results,
    )
    _print_summary(run)
    return run


def _print_summary(run: LoadTestResult) -> None:
    """Prints a human-readable summary so a single notebook cell tells the whole story."""

    client_times = [r.client_elapsed_ms for r in run.successes]

    print("=" * 56)
    print("{path}  |  users={users}  sleep={seconds}s".format(path=run.path, users=run.users, seconds=run.seconds))
    print("-" * 56)
    print("wall clock total : {ms} ms ({s:.2f} s)".format(ms=run.wall_clock_ms, s=run.wall_clock_ms / 1000))
    print("succeeded        : {ok}/{total}".format(ok=len(run.successes), total=run.users))
    if run.failures:
        print("failed           : {n} (first: {err})".format(n=len(run.failures), err=run.failures[0].error))
    if client_times:
        print("per-request (client-observed, ms):")
        print("  min  {min}   median {med}   mean {avg}   p95 {p95}   max {max}".format(
            min=min(client_times),
            med=int(median(client_times)),
            avg=int(mean(client_times)),
            p95=_percentile(client_times, 95),
            max=max(client_times),
        ))
    print("=" * 56)


if __name__ == "__main__":
    # Quick standalone smoke run; the notebook should import `load_test` instead.
    load_test(path="/api/v1/sleep/async", users=20, seconds=2)
    load_test(path="/api/v1/sleep/sync", users=20, seconds=2)
