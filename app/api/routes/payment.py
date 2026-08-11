from fastapi import APIRouter, Response, Header

from app.schemas.payment import CreatePaymentRequest

router = APIRouter(tags=["payments"])


@router.post("/payments", status_code=201)
async def charge_customer(
        data: CreatePaymentRequest,
        response: Response,
idempotency_key: str = Header(..., alias="Idempotency-Key"),
)