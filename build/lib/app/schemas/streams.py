from pydantic import BaseModel


class ActiveStreamsResponse(BaseModel):
    """
    Returns the number of currently active streaming responses in this process
    so callers can observe stream load without inspecting internal state.
    """

    count: int
