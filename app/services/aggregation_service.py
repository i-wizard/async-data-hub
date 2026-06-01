import asyncio
import json
import time
from typing import AsyncIterator, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app.cache.redis_cache import AsyncCache
from app.core.state import AppState
from app.db.repositories import AggregateRepository
from app.schemas.aggregation import (
    AggregationResponse,
    AggregationSourceResult,
    AggregationStreamEvent,
)
from app.streaming.sse import format_sse
from app.utils.http import SharedHttpClient
from app.utils.logger import CustomLogger
from app.websocket.manager import WebSocketManager


class AggregationService:
    """
    Orchestrates multi-source fan-out so the project demonstrates structured
    concurrency, partial failure handling, caching, and realtime updates.
    """

    SOURCE_NAMES = ["weather", "news", "finance"]

    def __init__(
        self,
        state: AppState,
        session: AsyncSession,
        cache: AsyncCache,
        http_client: SharedHttpClient,
        websocket_manager: WebSocketManager,
    ):
        """
        Receives all async collaborators explicitly so the workflow remains easy
        to reason about and individual pieces can be tested in isolation.
        """

        self._state = state
        self._session = session
        self._cache = cache
        self._http_client = http_client
        self._websocket_manager = websocket_manager
        self._repository = AggregateRepository(session=session)

    async def aggregate(self, query: str, use_cache: bool = True) -> AggregationResponse:
        """
        Returns one aggregate response, optionally through cache and request
        coalescing, because repeated identical fan-out should not multiply load.
        """

        cache_key = "aggregate:{query}".format(query=query)
        if use_cache:
            cached_payload = await self._cache.get(key=cache_key)
            if cached_payload is not None:
                return AggregationResponse(**cached_payload, cached=True)

            payload = await self._cache.get_or_set(
                key=cache_key,
                factory=lambda: self._build_response_payload(query=query),
                ttl_seconds=self._state.settings.cache_ttl_seconds,
            )
            return AggregationResponse(**payload, cached=False)

        payload = await self._build_response_payload(query=query)
        return AggregationResponse(**payload, cached=False)

    async def stream_aggregate(self, query: str) -> AsyncIterator[str]:
        """
        Streams source results as soon as they complete so the caller can see
        concurrency in action rather than waiting for a full batch response.
        """

        async with self._state.stream_counter.track():
            aggregate_request = await self._repository.create_request(query=query, status="RUNNING")
            await self._session.commit()

            yield format_sse(
                AggregationStreamEvent(
                    request_id=aggregate_request.id,
                    event="started",
                    query=query,
                ).model_dump(),
                event="started",
            )

            queue: asyncio.Queue[AggregationStreamEvent] = asyncio.Queue(maxsize=len(self.SOURCE_NAMES) + 1)
            completion_event = asyncio.Event()

            async def _run_source(source_name: str) -> None:
                result = await self._fetch_source(query=query, source_name=source_name)
                await queue.put(
                    AggregationStreamEvent(request_id=aggregate_request.id, event="source_result", source=result.source, query=query, payload=result.data, error=result.error),
                )
                await self._broadcast_update(query=query, event_name="source_result", payload=result.model_dump())

            async def _produce_results() -> None:
                try:
                    async with asyncio.TaskGroup() as task_group:
                        for source_name in self.SOURCE_NAMES:
                            task_group.create_task(_run_source(source_name=source_name))
                finally:
                    completion_event.set()

            producer_task = None
            try:
                producer_task = asyncio.create_task(_produce_results())

                while True:
                    if completion_event.is_set() and queue.empty():
                        break
                    event = await queue.get()
                    await self._repository.add_source_result(
                        aggregate_request_id=aggregate_request.id,
                        source_name=event.source or "unknown",
                        status="success" if event.error is None else "error",
                        payload=event.payload,
                        error_message=event.error,
                        duration_ms=None,
                    )
                    await self._session.commit()
                    yield format_sse(event.model_dump(), event=event.event)

                await producer_task
                await self._repository.mark_request_complete(
                    aggregate_request_id=aggregate_request.id,
                    status="COMPLETED",
                )
                await self._session.commit()

                yield format_sse(
                    AggregationStreamEvent(
                        request_id=aggregate_request.id,
                        event="completed",
                        query=query,
                    ).model_dump(),
                    event="completed",
                )
            except asyncio.CancelledError:
                if producer_task is not None:
                    # Cancel the orphan producer (sync, no await) so its source tasks stop
                    # before the request session is torn down and they hit DetachedInstanceError.
                    producer_task.cancel()
                # Schedule the DB cleanup in a detached task with a fresh session because
                # Starlette wraps the request in an anyio CancelScope that keeps re-cancelling.
                asyncio.create_task(
                    self._mark_cancelled_in_fresh_session(request_id=aggregate_request.id),
                )
                CustomLogger.info(f"Aggregation stream cancelled for request {aggregate_request.id}, cleanup scheduled")
                raise

    async def _mark_cancelled_in_fresh_session(self, request_id: int) -> None:
        """
        Marks an aggregate request CANCELLED using a fresh session so the cleanup
        survives the request's anyio cancel scope and its session teardown.
        """

        try:
            async with self._state.session_factory() as session:
                repo = AggregateRepository(session=session)
                await repo.mark_request_complete(aggregate_request_id=request_id, status="CANCELLED")
                await session.commit()
        except Exception as exc:
            CustomLogger.error(f"Failed to mark request {request_id} as CANCELLED: {exc}")

    async def _build_response_payload(self, query: str) -> Dict:
        """
        Creates the uncached aggregate payload so cached and non-cached flows
        share one implementation and stay behaviorally consistent.
        """

        aggregate_request = await self._repository.create_request(query=query, status="RUNNING")
        # (self._session.commit()) Commits the parent row now so the DB connection grabbed during flush returns to the pool
        # during the slow upstream fan-out below. Holding it open across that wait
        # pins one connection per in-flight request and starves the pool under load (many concurrent users).
        # A connection is acquired during flush and only returned to the pool on commit,
        # so flushing early and committing before the fan-out allows the workflow to scale even with a small connection pool (5 -15).
        await self._session.commit()
        results: List[AggregationSourceResult] = []

        async def _run_source(source_name: str) -> None:
            result = await self._fetch_source(query=query, source_name=source_name)
            results.append(result)
            await self._broadcast_update(query=query, event_name="aggregate_update", payload=result.model_dump())

        try:
            async with asyncio.TaskGroup() as task_group:
                for source_name in self.SOURCE_NAMES:
                    task_group.create_task(_run_source(source_name=source_name))
        except asyncio.CancelledError:
            # Detach the cleanup from Starlette's anyio cancel scope and from the
            # request session that's about to be torn down — same reasoning as
            # stream_aggregate. An awaited cleanup here would never run the UPDATE.
            asyncio.create_task(
                self._mark_cancelled_in_fresh_session(request_id=aggregate_request.id),
            )
            CustomLogger.info(f"Aggregation cancelled for request {aggregate_request.id}, cleanup scheduled")
            raise

        status = "COMPLETED"
        if any(result.status == "error" for result in results):
            status = "PARTIAL"

        for result in results:
            await self._repository.add_source_result(
                aggregate_request_id=aggregate_request.id,
                source_name=result.source,
                status=result.status,
                payload=result.data,
                error_message=result.error,
                duration_ms=result.duration_ms,
            )
        await self._repository.mark_request_complete(
            aggregate_request_id=aggregate_request.id,
            status=status,
        )
        await self._session.commit()
        results.sort(key=lambda item: item.source)
        return {
            "request_id": aggregate_request.id,
            "query": query,
            "status": status,
            "results": [result.model_dump() for result in results],
        }

    async def _fetch_source(self, query: str, source_name: str) -> AggregationSourceResult:
        """
        Fetches one source with timing and error capture so the aggregate layer
        can surface partial failures without aborting the full workflow.
        """

        started_at = time.perf_counter()
        try:
            source_url = "{base_url}/{source_name}".format(
                base_url=self._state.settings.aggregation_source_base_url.rstrip("/"),
                source_name=source_name,
            )
            payload = await self._http_client.get_json(url=source_url, params={"q": query})
            duration_ms = int((time.perf_counter() - started_at) * 1000)
            return AggregationSourceResult(
                source=source_name,
                status="success",
                data=payload,
                duration_ms=duration_ms,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            duration_ms = int((time.perf_counter() - started_at) * 1000)
            return AggregationSourceResult(
                source=source_name,
                status="error",
                error=str(exc),
                duration_ms=duration_ms,
            )

    async def _broadcast_update(self, query: str, event_name: str, payload: Dict) -> None:
        """
        Publishes workflow progress over WebSockets so users can observe async
        fan-out without tying the aggregation service to socket internals.
        """

        message = json.dumps({"event": event_name, "query": query, "payload": payload})
        await self._websocket_manager.broadcast(message=message)
