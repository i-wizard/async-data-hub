from typing import List

from fastapi import APIRouter, Depends, status

from app.config.dependencies import inventory_service
from app.schemas.inventory import (
    ProductResponse,
    ProductRequest,
    ReserveStrategy,
    ReserveRequest, ProductReservationResponse,
)
from app.services.inventory_service import InventoryService

router = APIRouter(prefix="/inventory")


@router.post(
    "/products", response_model=ProductResponse, status_code=status.HTTP_201_CREATED
)
async def add_product(
    data: ProductRequest,
    inventory_service_: InventoryService = Depends(inventory_service),
):
    response = await inventory_service_.add_product(name=data.name, stock=data.stock)
    return response


@router.get("/products", response_model=List[ProductResponse])
async def list_products(
    limit: int = 100,
    offset: int = 0,
    inventory_service_: InventoryService = Depends(inventory_service),
):
    response = await inventory_service_.list_products(limit=limit, offset=offset)
    return response


@router.get("/products/{product_id}", response_model=ProductResponse)
async def get_product(
    product_id: str,
    inventory_service_: InventoryService = Depends(inventory_service),
):
    response = await inventory_service_.get_product(product_id=product_id)
    return response


@router.get("/products/{product_id}/reservations/count", response_model=int)
async def count_reservations(
    product_id: str,
    inventory_service_: InventoryService = Depends(inventory_service),
):
    response = await inventory_service_.count_reservations(product_id=product_id)
    return response


@router.post("/products/{product_id}/reserve/{strategy}", response_model=ProductReservationResponse)
async def reserve_product(
    product_id: str,
    strategy: ReserveStrategy,
    data: ReserveRequest,
    inventory_service_: InventoryService = Depends(inventory_service),
):
    response = await inventory_service_.reserve_product(
        product_id=product_id, strategy=strategy, quantity=data.quantity
    )
    return response
