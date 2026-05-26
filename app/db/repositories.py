from datetime import datetime
from typing import List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import (
    AggregateRequestRecord,
    SourceResultRecord,
    WebhookDeliveryAttemptRecord,
    WebhookEventRecord,
)


class AggregateRepository:
    """
    Encapsulates aggregation persistence so service code stays focused on async
    orchestration instead of SQLAlchemy object lifecycle details.
    """

    def __init__(self, session: AsyncSession):
        """
        Keeps one request-scoped session so all writes for the workflow can
        participate in the same transactional context.
        """

        self._session = session

    async def create_request(self, query: str, status: str) -> AggregateRequestRecord:
        """
        Persists the aggregate request early because concurrent child work needs
        a durable parent identifier for partial result tracking.
        """

        record = AggregateRequestRecord(query=query, status=status)
        self._session.add(record)
        await self._session.flush()
        return record

    async def add_source_result(
        self,
        aggregate_request_id: int,
        source_name: str,
        status: str,
        payload: Optional[dict],
        error_message: Optional[str],
        duration_ms: Optional[int],
    ) -> SourceResultRecord:
        """
        Stores each source outcome independently so one failing upstream does not
        erase the evidence of other successful concurrent calls.
        """

        record = SourceResultRecord(
            aggregate_request_id=aggregate_request_id,
            source_name=source_name,
            status=status,
            payload=payload,
            error_message=error_message,
            duration_ms=duration_ms,
        )
        self._session.add(record)
        await self._session.flush()
        return record

    async def mark_request_complete(self, aggregate_request_id: int, status: str) -> None:
        """
        Finalizes the parent request status after child tasks finish so later
        inspection shows whether the workflow ended successfully or partially.
        """

        record = await self._session.get(AggregateRequestRecord, aggregate_request_id)
        if record is not None:
            record.status = status
            record.completed_at = datetime.utcnow()
            await self._session.flush()

    async def list_source_results(self, aggregate_request_id: int) -> List[SourceResultRecord]:
        """
        Fetches all source results in one query because async code should still
        avoid N+1 database access patterns.
        """

        statement = select(SourceResultRecord).where(
            SourceResultRecord.aggregate_request_id == aggregate_request_id,
        )
        result = await self._session.execute(statement)
        return list(result.scalars().all())


class WebhookRepository:
    """
    Encapsulates webhook persistence so retry and fan-out bookkeeping remains
    explicit and testable without leaking ORM mechanics into services.
    """

    def __init__(self, session: AsyncSession):
        """
        Uses the request-scoped session so event and attempt records can be
        committed together after the fan-out workflow completes.
        """

        self._session = session

    async def create_event(self, event_name: str, payload: dict, status: str) -> WebhookEventRecord:
        """
        Persists the parent webhook event first because each target delivery
        attempt needs a durable foreign-key anchor.
        """

        record = WebhookEventRecord(event_name=event_name, payload=payload, status=status)
        self._session.add(record)
        await self._session.flush()
        return record

    async def add_attempt(
        self,
        webhook_event_id: int,
        target_url: str,
        attempt_number: int,
        status: str,
        response_status_code: Optional[int],
        error_message: Optional[str],
    ) -> WebhookDeliveryAttemptRecord:
        """
        Writes every delivery attempt so retries become visible rather than being
        compressed into one final success-or-failure flag.
        """

        record = WebhookDeliveryAttemptRecord(
            webhook_event_id=webhook_event_id,
            target_url=target_url,
            attempt_number=attempt_number,
            status=status,
            response_status_code=response_status_code,
            error_message=error_message,
        )
        self._session.add(record)
        await self._session.flush()
        return record

    async def update_event_status(self, event_id: int, status: str) -> None:
        """
        Updates the parent event after all deliveries finish so API responses and
        persisted state agree on the workflow outcome.
        """

        record = await self._session.get(WebhookEventRecord, event_id)
        if record is not None:
            record.status = status
            await self._session.flush()
