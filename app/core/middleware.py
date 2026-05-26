import time
import uuid

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

from app.utils.logger import CustomLogger


class RequestContextMiddleware(BaseHTTPMiddleware):
    """
    Adds a per-request correlation identifier so concurrent async flows can be
    traced across logs without relying on shared mutable state.
    """

    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("x-request-id", str(uuid.uuid4()))
        request.state.request_id = request_id
        start_time = time.perf_counter()

        response = await call_next(request)
        duration_ms = (time.perf_counter() - start_time) * 1000
        response.headers["x-request-id"] = request_id

        CustomLogger.info(
            "request_completed",
            extra={
                "request_id": request_id,
                "path": request.url.path,
                "method": request.method,
                "status_code": response.status_code,
                "duration_ms": round(duration_ms, 2),
            },
        )
        return response
