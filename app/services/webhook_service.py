import asyncio
from typing import List

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.state import AppState
from app.db.repositories import WebhookRepository
from app.schemas.webhooks import TriggerWebhookRequest, TriggerWebhookResponse, WebhookAttemptResponse
from app.utils.http import SharedHttpClient
from app.websocket.manager import WebSocketManager


class WebhookService:
    """
    Handles bounded webhook fan-out so the project demonstrates retry policy,
    timeout handling, and persistence of distributed delivery attempts.
    """

    def __init__(
        self,
        state: AppState,
        session: AsyncSession,
        http_client: SharedHttpClient,
        websocket_manager: WebSocketManager,
    ):
        """
        Receives shared async collaborators so the fan-out workflow can enforce
        process-wide limits and publish progress updates consistently.
        """

        self._state = state
        self._session = session
        self._http_client = http_client
        self._websocket_manager = websocket_manager
        self._repository = WebhookRepository(session=session)

    async def trigger_event(self, data: TriggerWebhookRequest) -> TriggerWebhookResponse:
        """
        Fans out one event to many targets under a shared limiter because
        unbounded outbound webhook bursts are a common async scalability bug.
        """

        event_record = await self._repository.create_event(
            event_name=data.event_name,
            payload=data.payload,
            status="RUNNING",
        )
        await self._session.commit()
        attempts: List[WebhookAttemptResponse] = []

        async def _deliver(target_url: str) -> None:
            # Each delivery opens its own session because AsyncSession is not safe
            # for concurrent use — concurrent flushes from sibling tasks on the same
            # session raise "Session is already flushing".
            async with self._state.session_factory() as session:
                repo = WebhookRepository(session=session)
                for attempt_number in [1, 2]:
                    try:
                        async with self._state.webhook_limiter.slot():
                            response = await self._http_client.post_json(url=target_url, payload=data.payload)
                        attempt = await repo.add_attempt(
                            webhook_event_id=event_record.id,
                            target_url=target_url,
                            attempt_number=attempt_number,
                            status="success",
                            response_status_code=response.status_code,
                            error_message=None,
                        )
                        await session.commit()
                        attempts.append(
                            WebhookAttemptResponse(
                                target_url=attempt.target_url,
                                attempt_number=attempt.attempt_number,
                                status=attempt.status,
                                response_status_code=attempt.response_status_code,
                            ),
                        )
                        await self._websocket_manager.broadcast(
                            message='{"event":"webhook_delivery","target_url":"%s","status":"success"}' % target_url,
                        )
                        return
                    except asyncio.CancelledError:
                        raise
                    except httpx.HTTPError as exc:
                        failed_attempt = await repo.add_attempt(
                            webhook_event_id=event_record.id,
                            target_url=target_url,
                            attempt_number=attempt_number,
                            status="failed",
                            response_status_code=getattr(exc.response, "status_code", None),
                            error_message=str(exc),
                        )
                        await session.commit()
                        attempts.append(
                            WebhookAttemptResponse(
                                target_url=failed_attempt.target_url,
                                attempt_number=failed_attempt.attempt_number,
                                status=failed_attempt.status,
                                response_status_code=failed_attempt.response_status_code,
                                error_message=failed_attempt.error_message,
                            ),
                        )
                        if attempt_number == 1:
                            await asyncio.sleep(0.1)

        try:
            async with asyncio.TaskGroup() as task_group:
                for target in data.targets:
                    task_group.create_task(_deliver(target_url=str(target)))
        except asyncio.CancelledError:
            await self._repository.update_event_status(event_id=event_record.id, status="CANCELLED")
            await self._session.commit()
            raise

        final_status = "COMPLETED"
        if any(attempt.status == "failed" for attempt in attempts):
            successful_targets = {attempt.target_url for attempt in attempts if attempt.status == "success"}
            all_targets = {str(target) for target in data.targets}
            if successful_targets != all_targets:
                final_status = "PARTIAL"

        await self._repository.update_event_status(event_id=event_record.id, status=final_status)
        await self._session.commit()
        return TriggerWebhookResponse(event_id=event_record.id, status=final_status, attempts=attempts)
