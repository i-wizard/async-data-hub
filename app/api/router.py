from fastapi import APIRouter

from app.api.routes.aggregation import router as aggregation_router
from app.api.routes.analysis import router as analysis_router
from app.api.routes.health import router as health_router
from app.api.routes.mock_webhooks import router as mock_webhooks_router
from app.api.routes.mock_sources import router as mock_sources_router
from app.api.routes.webhooks import router as webhooks_router
from app.api.routes.websocket import router as websocket_router

api_router = APIRouter(prefix="/api/v1")
api_router.include_router(health_router, tags=["health"])
api_router.include_router(aggregation_router, tags=["aggregation"])
api_router.include_router(webhooks_router, tags=["webhooks"])
api_router.include_router(analysis_router, tags=["analysis"])
api_router.include_router(mock_sources_router, tags=["mock-sources"])
api_router.include_router(mock_webhooks_router, tags=["mock-webhooks"])
api_router.include_router(websocket_router, tags=["websocket"])
