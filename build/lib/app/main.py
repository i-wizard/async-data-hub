from collections import deque

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.api.router import api_router
from app.api.routes.observability import SimulatedFailure
from app.config.settings import get_settings
from app.core.lifespan import lifespan
from app.core.middleware import RequestContextMiddleware
from app.services.observability import OrderNotFound


def create_app() -> FastAPI:
    """
    Builds the FastAPI application through a factory so tests and production
    workers create identical app instances with lifespan support.
    """

    settings = get_settings()
    app = FastAPI(title=settings.app_name, lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.frontend_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    # app.add_middleware(RequestContextMiddleware)
    app.include_router(api_router)

    app.exception_handler(OrderNotFound)
    async def _handle_not_found(_: Request, exc: OrderNotFound) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"detail": f"order {exc} not found", "code": "order_not_found"},
        )
    app.exception_handler(SimulatedFailure)
    async def _handle_simulated_failure(_: Request, exc: SimulatedFailure) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": f"simulated failure: {exc}", "code": "simulated_failure"},
        )


    # Observability deps
    app.state.orders = {}
    app.state.samples = deque(maxlen=1_000)
    return app


app = create_app()
