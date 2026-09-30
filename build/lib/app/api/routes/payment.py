from fastapi import APIRouter, Response, Header, Depends, Query

from app.config.dependencies import payment_service
from app.schemas.payment import CreatePaymentRequest, PaymentResponse
from app.services.payment_service import PaymentService

router = APIRouter(tags=["payments"])


@router.post("/payments", status_code=201)
async def charge_customer(
    data: CreatePaymentRequest,
    response: Response,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    payment_service_: PaymentService = Depends(payment_service),
):
    result = await payment_service_.charge_with_idempotency(data, idempotency_key)
    if result["idempotent_replayed"]:
        response.headers["X-Idempotent-Replayed"] = "true"
    return result


@router.post("/payments/buggy", status_code=201)
async def charge_customer_buggy(
    data: CreatePaymentRequest,
    response: Response,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    bug: str =  Query(default="replay", description="Choose the bug to simulate: 'replay' or 'duplicate'"),
    payment_service_: PaymentService = Depends(payment_service),
):
    result = await payment_service_.charge_with_bug(data, idempotency_key, bug=bug)
    if result["idempotent_replayed"]:
        response.headers["X-Idempotent-Replayed"] = "true"
    return result