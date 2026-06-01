from typing import Optional

from fastapi import APIRouter, Depends, Header, Query
from fastapi.responses import StreamingResponse

from app.config.dependencies import media_service
from app.schemas.media import MediaListResponse
from app.services.media_service import MediaService

router = APIRouter(prefix="/media")


@router.get(
    "/list",
    response_model=MediaListResponse,
    response_description="List sample media files available for streaming",
)
def list_media(_service: MediaService = Depends(media_service)) -> MediaListResponse:
    """
    Returns the names of files in the configured samples directory so a UI can
    pick one without coupling its dropdown to the running server's filesystem.
    """

    return MediaListResponse(files=_service.list_files())


@router.get(
    "/progressive/{filename}",
    response_model=None,
    response_class=StreamingResponse,
    response_description="Plain chunked progressive stream with no seek support",
)
def progressive_media(
    filename: str,
    _service: MediaService = Depends(media_service),
) -> StreamingResponse:
    """
    Streams the file from start to end so users can compare the no-seek pattern
    against the Range-aware endpoint side by side.
    """

    return _service.progressive_response(filename=filename)


@router.get(
    "/seekable/{filename}",
    response_model=None,
    response_class=StreamingResponse,
    response_description="Range-aware stream that responds 206 to Range requests",
)
def seekable_media(
    filename: str,
    range_header: Optional[str] = Header(default=None, alias="Range"),
    _service: MediaService = Depends(media_service),
) -> StreamingResponse:
    """
    Honors HTTP Range requests so browser players can scrub through the file by
    asking for byte ranges instead of refetching the whole asset.
    """

    return _service.seekable_response(filename=filename, range_header=range_header)


@router.get(
    "/remote",
    response_model=None,
    response_class=StreamingResponse,
    response_description="Proxy stream a direct remote media URL",
)
async def remote_media(
    url: str = Query(..., min_length=1),
    range_header: Optional[str] = Header(default=None, alias="Range"),
    _service: MediaService = Depends(media_service),
) -> StreamingResponse:
    """
    Streams a direct remote media asset through the API so users can study how
    proxy streaming, byte ranges, and cancellation behave with upstream I/O.
    """

    return await _service.remote_response(url=url, range_header=range_header)
