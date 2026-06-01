import asyncio
import ipaddress
import mimetypes
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Dict, List, Optional
from urllib.parse import urlparse

import aiofiles
import httpx
from fastapi import HTTPException
from fastapi.responses import StreamingResponse

from app.core.state import AppState
from app.utils.logger import CustomLogger

ALLOWED_EXTENSIONS = {".mp4", ".mp3", ".webm", ".ogg", ".wav", ".m4a"}


@dataclass
class ByteRange:
    """
    Represents one resolved HTTP byte-range so the streaming layer can read
    exactly that slice without re-parsing the header inside the I/O loop.
    """

    start: int
    end: int
    total: int

    @property
    def length(self) -> int:
        return self.end - self.start + 1


class MediaService:
    """
    Resolves media files, parses Range headers, and streams chunks so the route
    handlers stay focused on HTTP concerns rather than file I/O details.
    """

    def __init__(self, state: AppState):
        """
        Stores the resolved samples directory once at construction because the
        path-traversal guard relies on comparing resolved paths consistently.
        """

        self._stream_counter = state.stream_counter
        self._http_client = state.http_client
        self._samples_dir = Path(state.settings.media_samples_dir).resolve()
        self._chunk_size = state.settings.media_chunk_size
        self._remote_chunk_size = state.settings.remote_media_chunk_size
        self._remote_allowed_schemes = set(state.settings.remote_media_allowed_schemes)
        self._remote_allowed_hosts = set(state.settings.remote_media_allowed_hosts)
        self._remote_block_private_hosts = state.settings.remote_media_block_private_hosts

    def list_files(self) -> List[str]:
        """
        Returns whitelisted filenames in the samples directory so the frontend
        can populate its picker without hardcoding filenames.
        """

        if not self._samples_dir.is_dir():
            return []
        return sorted(
            entry.name
            for entry in self._samples_dir.iterdir()
            if entry.is_file() and entry.suffix.lower() in ALLOWED_EXTENSIONS
        )

    def resolve_safe_path(self, filename: str) -> Path:
        """
        Resolves a flat filename under the samples directory so path-traversal
        attempts and unknown extensions cannot reach the file-streaming layer.
        """

        if not filename or "/" in filename or "\\" in filename or filename in (".", ".."):
            raise HTTPException(status_code=404)
        candidate = (self._samples_dir / filename).resolve()
        if not candidate.is_relative_to(self._samples_dir):
            raise HTTPException(status_code=404)
        if candidate.suffix.lower() not in ALLOWED_EXTENSIONS:
            raise HTTPException(status_code=404)
        if not candidate.is_file():
            raise HTTPException(status_code=404)
        return candidate

    def progressive_response(self, filename: str) -> StreamingResponse:
        """
        Builds a 200 chunked response that reads the whole file front-to-back so
        the comparison endpoint demonstrates a streaming response without seek.
        """

        path = self.resolve_safe_path(filename=filename)
        total = path.stat().st_size
        return StreamingResponse(
            self._stream_full_file(path=path),
            media_type=self._guess_content_type(path=path),
            headers={"Content-Length": str(total)},
        )

    def seekable_response(self, filename: str, range_header: Optional[str]) -> StreamingResponse:
        """
        Builds a Range-aware response so browser media players can seek by
        requesting partial byte ranges instead of always re-fetching the file.
        """
        CustomLogger.info(f"Range : {range_header}")
        path = self.resolve_safe_path(filename=filename)
        media_type = self._guess_content_type(path=path)
        total = path.stat().st_size

        if range_header is None:
            return StreamingResponse(
                self._stream_full_file(path=path),
                media_type=media_type,
                headers={
                    "Content-Length": str(total),
                    "Accept-Ranges": "bytes",
                },
            )

        byte_range = self._parse_range(header=range_header, total=total)
        CustomLogger.info(f"byte_range: {byte_range}")
        return StreamingResponse(
            self._stream_byte_range(path=path, byte_range=byte_range),
            status_code=206,
            media_type=media_type,
            headers={
                "Content-Range": "bytes {start}-{end}/{total}".format(
                    start=byte_range.start, end=byte_range.end, total=byte_range.total,
                ),
                "Content-Length": str(byte_range.length),
                "Accept-Ranges": "bytes",
            },
        )

    async def remote_response(self, url: str, range_header: Optional[str]) -> StreamingResponse:
        """
        Opens a remote direct-media URL and returns a downstream stream so the
        API can demonstrate proxy streaming without buffering the whole asset.
        """

        await self._validate_remote_url(url=url)
        request_headers = {}
        if range_header is not None:
            request_headers["Range"] = range_header

        try:
            request = self._http_client.build_request(
                method="GET",
                url=url,
                headers=request_headers,
            )
            upstream_response = await self._http_client.send(request=request, stream=True)
        except asyncio.CancelledError:
            raise
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=502,
                detail="Unable to open remote media stream: {error}".format(error=str(exc)),
            ) from exc

        if upstream_response.status_code == 416:
            await upstream_response.aclose()
            raise HTTPException(
                status_code=416,
                detail="Remote server rejected the requested range",
            )
        if upstream_response.status_code == 404:
            await upstream_response.aclose()
            raise HTTPException(status_code=404, detail="Remote media was not found")
        if upstream_response.status_code not in (200, 206):
            status_code = upstream_response.status_code
            await upstream_response.aclose()
            raise HTTPException(
                status_code=502,
                detail="Remote media returned unsupported status {status_code}".format(
                    status_code=status_code,
                ),
            )

        response_headers = self._remote_response_headers(upstream_response=upstream_response)
        media_type = response_headers.pop("Content-Type", None) or "application/octet-stream"
        return StreamingResponse(
            self._stream_remote_response(upstream_response=upstream_response),
            status_code=upstream_response.status_code,
            media_type=media_type,
            headers=response_headers,
        )

    async def _stream_full_file(self, path: Path) -> AsyncIterator[bytes]:
        """
        Yields the file in fixed-size chunks via aiofiles so reads run in a
        threadpool and the event loop stays responsive during large transfers.
        """

        async with self._stream_counter.track():
            async with aiofiles.open(path, mode="rb") as file:
                while True:
                    chunk = await file.read(self._chunk_size)
                    if not chunk:
                        break
                    yield chunk

    async def _stream_byte_range(self, path: Path, byte_range: ByteRange) -> AsyncIterator[bytes]:
        """
        Yields only the requested byte range so 206 responses send exactly the
        bytes the client asked for instead of the entire file.
        """

        async with self._stream_counter.track():
            remaining = byte_range.length
            async with aiofiles.open(path, mode="rb") as file:
                await file.seek(byte_range.start)
                while remaining > 0:
                    chunk = await file.read(min(self._chunk_size, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
                    yield chunk

    async def _stream_remote_response(self, upstream_response: httpx.Response) -> AsyncIterator[bytes]:
        """
        Yields remote bytes as they arrive and closes the upstream response when
        the browser finishes, disconnects, or cancels the downstream stream.
        """

        async with self._stream_counter.track():
            try:
                async for chunk in upstream_response.aiter_bytes(chunk_size=self._remote_chunk_size):
                    if chunk:
                        yield chunk
            except asyncio.CancelledError:
                raise
            finally:
                await upstream_response.aclose()

    async def _validate_remote_url(self, url: str) -> None:
        """
        Rejects unsafe remote URLs before outbound I/O because URL proxy
        endpoints can otherwise become server-side request forgery primitives.
        """

        parsed = urlparse(url)
        if parsed.scheme not in self._remote_allowed_schemes:
            raise HTTPException(status_code=400, detail="Unsupported remote media URL scheme")
        if not parsed.hostname:
            raise HTTPException(status_code=400, detail="Remote media URL must include a host")

        host = parsed.hostname.lower()
        if self._remote_allowed_hosts and host not in self._remote_allowed_hosts:
            raise HTTPException(status_code=400, detail="Remote media host is not allowed")
        if host in self._remote_allowed_hosts:
            return
        if self._remote_block_private_hosts:
            await self._reject_private_host(host=host)

    async def _reject_private_host(self, host: str) -> None:
        """
        Blocks localhost and private network destinations so a remote streaming
        endpoint cannot be used to reach internal services from the API server.
        """

        if host in {"localhost", "localhost.localdomain"}:
            raise HTTPException(status_code=400, detail="Private remote media hosts are blocked")

        try:
            literal_ip = ipaddress.ip_address(host)
        except ValueError:
            literal_ip = None

        if literal_ip is not None:
            if self._is_private_address(address=literal_ip):
                raise HTTPException(status_code=400, detail="Private remote media hosts are blocked")
            return

        try:
            address_infos = await asyncio.to_thread(socket.getaddrinfo, host, None)
        except socket.gaierror as exc:
            raise HTTPException(status_code=400, detail="Remote media host could not be resolved") from exc

        for address_info in address_infos:
            resolved_host = address_info[4][0]
            try:
                resolved_ip = ipaddress.ip_address(resolved_host)
            except ValueError:
                continue
            if self._is_private_address(address=resolved_ip):
                raise HTTPException(status_code=400, detail="Private remote media hosts are blocked")

    @staticmethod
    def _guess_content_type(path: Path) -> str:
        """
        Maps the file extension to a MIME type via the standard library so we do
        not have to maintain our own extension-to-content-type table.
        """

        guessed, _ = mimetypes.guess_type(url=str(path))
        return guessed or "application/octet-stream"

    @staticmethod
    def _remote_response_headers(upstream_response: httpx.Response) -> Dict[str, str]:
        """
        Copies only media-relevant upstream headers so the browser receives byte
        range metadata without inheriting unrelated proxy or server headers.
        """

        response_headers = {}
        for header_name in ["Content-Type", "Content-Length", "Content-Range", "Accept-Ranges"]:
            value = upstream_response.headers.get(header_name)
            if value is not None:
                response_headers[header_name] = value
        return response_headers

    @staticmethod
    def _is_private_address(address: ipaddress._BaseAddress) -> bool:
        """
        Groups non-public address checks in one place so SSRF protections remain
        readable as the remote streaming rules evolve.
        """

        return (
            address.is_private
            or address.is_loopback
            or address.is_link_local
            or address.is_multicast
            or address.is_reserved
            or address.is_unspecified
        )

    @staticmethod
    def _parse_range(header: str, total: int) -> ByteRange:
        """
        Parses a single-range HTTP Range header into byte offsets so the stream
        layer can seek and read exactly the requested slice.
        """

        prefix = "bytes="
        if not header.startswith(prefix):
            raise HTTPException(status_code=416, detail="Invalid Range header")
        spec = header[len(prefix):].strip()
        if "," in spec:
            raise HTTPException(status_code=416, detail="Multi-range not supported")

        start_str, separator, end_str = spec.partition("-")
        if not separator:
            raise HTTPException(status_code=416, detail="Invalid Range header")

        try:
            if start_str == "" and end_str != "":
                suffix_length = int(end_str)
                if suffix_length <= 0:
                    raise ValueError
                start = max(0, total - suffix_length)
                end = total - 1
            elif start_str != "" and end_str == "":
                start = int(start_str)
                end = total - 1
            elif start_str != "" and end_str != "":
                start = int(start_str)
                end = min(int(end_str), total - 1)
            else:
                raise ValueError
        except ValueError:
            raise HTTPException(status_code=416, detail="Invalid Range header")

        if start < 0 or start >= total or end < start:
            raise HTTPException(
                status_code=416,
                detail="Range out of bounds for size {total}".format(total=total),
                headers={"Content-Range": "bytes */{total}".format(total=total)},
            )
        return ByteRange(start=start, end=end, total=total)
