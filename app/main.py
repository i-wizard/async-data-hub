from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.router import api_router
from app.config.settings import get_settings
from app.core.lifespan import lifespan
from app.core.middleware import RequestContextMiddleware


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
    app.add_middleware(RequestContextMiddleware)
    app.include_router(api_router)
    return app


app = create_app()
