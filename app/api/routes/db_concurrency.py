from fastapi import APIRouter, Depends, Query

from app.config.dependencies import db_concurrency_service
from app.schemas.db_concurrency import (
    AccountResponse,
    CreateAccountRequest,
    TransferResponse,
    TransferRequest,
    IsolationLevel,
)
from app.services.db_concurrency_service import DBConcurrencyService

router = APIRouter(tags=["banking"])


@router.post(
    "/accounts",
    response_model=AccountResponse,
    summary="Open an account with an initial balance.",
)
async def open_account(
    data: CreateAccountRequest,
    _service: DBConcurrencyService = Depends(db_concurrency_service),
):
    return await _service.open_account(data.id, owner=data.owner, balance=data.balance)


@router.get(
    "/accounts/{account_id}",
    response_model=AccountResponse,
    summary="Read an account's current balance.",
)
async def get_account(
    account_id: str, _service: DBConcurrencyService = Depends(db_concurrency_service)
):
    return await _service.get_account(account_id=account_id)


@router.post("/transfers", response_model=TransferResponse, status_code=201)
async def create_transfer(
    data: TransferRequest,
    isolation_level: IsolationLevel = Query(IsolationLevel.SERIALIZABLE),
    _service: DBConcurrencyService = Depends(db_concurrency_service),
):
    return await _service.transfer(
        from_id=data.from_id,
        to_id=data.to_id,
        amount=data.amount,
        isolation=isolation_level,
    )
