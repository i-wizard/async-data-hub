from fastapi import APIRouter, Depends

from app.config.dependencies import app_state
from app.core.state import AppState
from app.schemas.streams import ActiveStreamsResponse

router = APIRouter()


@router.get(
    "/streams/active",
    response_model=ActiveStreamsResponse,
    response_description="Returns count of currently-active streaming clients",
)
async def active_streams(
    state: AppState = Depends(app_state),
) -> ActiveStreamsResponse:
    """
    Reads the process-wide stream counter so learning clients can see how many
    streaming responses are actively being iterated right now.
    """

    return ActiveStreamsResponse(count=await state.stream_counter.get())
