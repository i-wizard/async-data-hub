import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import JSON, CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, text as sql_text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import UUID
from uuid6 import uuid7

from app.db.base import Base


def current_datetime() -> datetime:
    return datetime.now(timezone.utc)


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
    source_results: Mapped[list["SourceResultRecord"]] = relationship(
        back_populates="aggregate_request"
    )


class SourceResultRecord(Base):
    """
    Stores one upstream result per source so partial successes and failures stay
    visible even when concurrent fan-out does not fully succeed.
    """

    __tablename__ = "source_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    aggregate_request_id: Mapped[int] = mapped_column(
        ForeignKey("aggregate_requests.id")
    )
    source_name: Mapped[str] = mapped_column(String(100), index=True)
    status: Mapped[str] = mapped_column(String(50), index=True)
    payload: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    duration_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    aggregate_request: Mapped[AggregateRequestRecord] = relationship(
        back_populates="source_results"
    )


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
    attempts: Mapped[list["WebhookDeliveryAttemptRecord"]] = relationship(
        back_populates="event"
    )


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


class CustomerAccount(Base):
    __tablename__ = 'customer_accounts'
    __table_args__ = (
        CheckConstraint("balance >= 0", name="ck_customer_accounts_balance_non_negative"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid7)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    balance: Mapped[int] = mapped_column(Integer, nullable=False, default=0)  # minor units (cents)


class Charge(Base):
    """
    A single money movement. One successful payment == exactly one row.
    """

    __tablename__ = "charges"
    __table_args__ = (
        # Only one SUCCEEDED charge per idempotency_key; failed attempts may
        # share a key so a client can safely retry after a failure.
        Index(
            "uq_charges_idempotency_key_succeeded",
            "idempotency_key",
            unique=True,
            postgresql_where=(sql_text("status = 'SUCCEEDED'")),
        ),
    )

    id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: f"ch_{uuid.uuid4().hex}"
    )
    amount: Mapped[int] = mapped_column(Integer, nullable=False)  # minor units (cents)
    customer_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("customer_accounts.id"), nullable=False)
    status: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=current_datetime
    )
    idempotency_key: Mapped[str] = mapped_column(String, nullable=False)
    error_message: Mapped[str] = mapped_column(String, nullable=True)
