from typing import List

from pydantic import BaseModel


class MediaListResponse(BaseModel):
    """
    Returns the names of media files available for streaming so the frontend can
    populate its picker without hardcoding filenames against server state.
    """

    files: List[str]
