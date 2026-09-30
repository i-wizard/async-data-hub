from pydantic import BaseModel


class MessageResponse(BaseModel):
    """
    Provides a small reusable response envelope for endpoints that need to
    confirm an action without returning a larger resource document.
    """

    message: str
