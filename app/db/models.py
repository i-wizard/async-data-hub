from datetime import datetime
from typing import Optional

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class AggregateRequestRecord(Base):
    """
    Stores the top-level aggregation request so source-level results can be tied
    back to one durable workflow for debugging and later feature expansion.
    """

    __tablename__ = "aggregate_requests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    query: Mapped[str] = mapped_column(String(255), index=True)
    status: Mapped[str] = mapped_column(String(50), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    source_results: Mapped[list["SourceResultRecord"]] = relationship(back_populates="aggregate_request")


class SourceResultRecord(Base):
    """
    Stores one upstream result per source so partial successes and failures stay
    visible even when concurrent fan-out does not fully succeed.
    """

    __tablename__ = "source_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    aggregate_request_id: Mapped[int] = mapped_column(ForeignKey("aggregate_requests.id"))
    source_name: Mapped[str] = mapped_column(String(100), index=True)
    status: Mapped[str] = mapped_column(String(50), index=True)
    payload: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    aggregate_request: Mapped[AggregateRequestRecord] = relationship(back_populates="source_results")


class WebhookEventRecord(Base):
    """
    Stores a webhook event independently from delivery attempts because fan-out
    to many targets is naturally a parent-child workflow.
    """

    __tablename__ = "webhook_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_name: Mapped[str] = mapped_column(String(100), index=True)
    payload: Mapped[dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(50), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    attempts: Mapped[list["WebhookDeliveryAttemptRecord"]] = relationship(back_populates="event")


class WebhookDeliveryAttemptRecord(Base):
    """
    Records each target delivery attempt so retries and timeout behavior can be
    inspected after the request has already returned.
    """

    __tablename__ = "webhook_delivery_attempts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    webhook_event_id: Mapped[int] = mapped_column(ForeignKey("webhook_events.id"))
    target_url: Mapped[str] = mapped_column(String(500))
    attempt_number: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(50), index=True)
    response_status_code: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    event: Mapped[WebhookEventRecord] = relationship(back_populates="attempts")
