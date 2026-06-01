from pathlib import Path
from typing import Iterator, Tuple
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.services.media_service import MediaService

MEDIA_BASE_URL = "/api/v1/media"
REMOTE_MEDIA_URL = "http://testserver/api/v1/mock-sources/media.mp4"
SAMPLE_VIDEO_NAME = "tiny.mp4"
SAMPLE_AUDIO_NAME = "song.mp3"
DISALLOWED_NAME = "ignored.txt"


@pytest.fixture
def media_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> Iterator[Tuple[TestClient, Path]]:
    """
    Wires MEDIA_SAMPLES_DIR before the app boots so the media endpoints serve
    files written by this fixture rather than whatever is in the repo.
    """

    samples_dir = tmp_path / "media"
    samples_dir.mkdir()
    (samples_dir / SAMPLE_VIDEO_NAME).write_bytes(bytes(range(256)) * 16)
    (samples_dir / SAMPLE_AUDIO_NAME).write_bytes(b"ID3" + bytes(1021))
    (samples_dir / DISALLOWED_NAME).write_text("not a media file")
    monkeypatch.setenv("MEDIA_SAMPLES_DIR", str(samples_dir))
    client: TestClient = request.getfixturevalue("client")
    yield client, samples_dir


def test_list_returns_only_whitelisted_files(media_client):
    client, _ = media_client
    response = client.get("{base}/list".format(base=MEDIA_BASE_URL))
    assert response.status_code == 200
    files = response.json()["files"]
    assert files == sorted([SAMPLE_AUDIO_NAME, SAMPLE_VIDEO_NAME])
    assert DISALLOWED_NAME not in files


def test_progressive_returns_full_file(media_client):
    client, samples_dir = media_client
    expected = (samples_dir / SAMPLE_VIDEO_NAME).read_bytes()
    response = client.get("{base}/progressive/{name}".format(base=MEDIA_BASE_URL, name=SAMPLE_VIDEO_NAME))
    assert response.status_code == 200
    assert response.content == expected
    assert response.headers["content-type"] == "video/mp4"
    assert response.headers["content-length"] == str(len(expected))


def test_seekable_without_range_returns_200(media_client):
    client, samples_dir = media_client
    expected = (samples_dir / SAMPLE_VIDEO_NAME).read_bytes()
    response = client.get("{base}/seekable/{name}".format(base=MEDIA_BASE_URL, name=SAMPLE_VIDEO_NAME))
    assert response.status_code == 200
    assert response.content == expected
    assert response.headers["accept-ranges"] == "bytes"


def test_seekable_with_range_returns_206(media_client):
    client, samples_dir = media_client
    full = (samples_dir / SAMPLE_VIDEO_NAME).read_bytes()
    total = len(full)
    response = client.get(
        "{base}/seekable/{name}".format(base=MEDIA_BASE_URL, name=SAMPLE_VIDEO_NAME),
        headers={"Range": "bytes=10-19"},
    )
    assert response.status_code == 206
    assert response.headers["content-range"] == "bytes 10-19/{total}".format(total=total)
    assert response.headers["content-length"] == "10"
    assert response.headers["accept-ranges"] == "bytes"
    assert response.content == full[10:20]


def test_seekable_open_ended_range(media_client):
    client, samples_dir = media_client
    full = (samples_dir / SAMPLE_VIDEO_NAME).read_bytes()
    total = len(full)
    response = client.get(
        "{base}/seekable/{name}".format(base=MEDIA_BASE_URL, name=SAMPLE_VIDEO_NAME),
        headers={"Range": "bytes=100-"},
    )
    assert response.status_code == 206
    assert response.headers["content-range"] == "bytes 100-{end}/{total}".format(
        end=total - 1, total=total,
    )
    assert response.content == full[100:]


def test_seekable_suffix_range(media_client):
    client, samples_dir = media_client
    full = (samples_dir / SAMPLE_VIDEO_NAME).read_bytes()
    total = len(full)
    response = client.get(
        "{base}/seekable/{name}".format(base=MEDIA_BASE_URL, name=SAMPLE_VIDEO_NAME),
        headers={"Range": "bytes=-50"},
    )
    assert response.status_code == 206
    assert response.content == full[-50:]
    assert response.headers["content-range"] == "bytes {start}-{end}/{total}".format(
        start=total - 50, end=total - 1, total=total,
    )


def test_seekable_out_of_bounds_range_returns_416(media_client):
    client, _ = media_client
    response = client.get(
        "{base}/seekable/{name}".format(base=MEDIA_BASE_URL, name=SAMPLE_VIDEO_NAME),
        headers={"Range": "bytes=999999999-"},
    )
    assert response.status_code == 416


def test_seekable_malformed_range_returns_416(media_client):
    client, _ = media_client
    response = client.get(
        "{base}/seekable/{name}".format(base=MEDIA_BASE_URL, name=SAMPLE_VIDEO_NAME),
        headers={"Range": "items=0-99"},
    )
    assert response.status_code == 416


def test_unknown_filename_returns_404(media_client):
    client, _ = media_client
    response = client.get("{base}/progressive/missing.mp4".format(base=MEDIA_BASE_URL))
    assert response.status_code == 404


def test_disallowed_extension_returns_404(media_client):
    client, _ = media_client
    response = client.get(
        "{base}/progressive/{name}".format(base=MEDIA_BASE_URL, name=DISALLOWED_NAME),
    )
    assert response.status_code == 404


def test_service_rejects_traversal_attempts(tmp_path: Path):
    """
    Validates path-traversal rejection at the service layer because the route
    parameter alone cannot express every shape we want to refuse.
    """

    samples_dir = tmp_path / "media"
    samples_dir.mkdir()
    (samples_dir / SAMPLE_VIDEO_NAME).write_bytes(b"\x00" * 16)
    state = MagicMock()
    state.settings.media_samples_dir = str(samples_dir)
    state.settings.media_chunk_size = 4096

    service = MediaService(state=state)

    for bad in ["", ".", "..", "../escape.mp4", "subdir/inside.mp4", "evil\\file.mp4"]:
        with pytest.raises(HTTPException) as exc_info:
            service.resolve_safe_path(filename=bad)
        assert exc_info.value.status_code == 404


def test_remote_stream_returns_full_content(client):
    response = client.get(
        "{base}/remote".format(base=MEDIA_BASE_URL),
        params={"url": REMOTE_MEDIA_URL},
    )
    assert response.status_code == 200
    assert response.content == bytes(range(256)) * 16
    assert response.headers["content-type"] == "video/mp4"
    assert response.headers["content-length"] == str(len(bytes(range(256)) * 16))
    assert response.headers["accept-ranges"] == "bytes"


def test_remote_stream_forwards_range_header(client):
    response = client.get(
        "{base}/remote".format(base=MEDIA_BASE_URL),
        params={"url": REMOTE_MEDIA_URL},
        headers={"Range": "bytes=10-19"},
    )
    assert response.status_code == 206
    assert response.content == (bytes(range(256)) * 16)[10:20]
    assert response.headers["content-range"] == "bytes 10-19/4096"
    assert response.headers["content-length"] == "10"


def test_remote_stream_rejects_invalid_scheme(client):
    response = client.get(
        "{base}/remote".format(base=MEDIA_BASE_URL),
        params={"url": "file:///tmp/video.mp4"},
    )
    assert response.status_code == 400


def test_remote_stream_rejects_missing_host(client):
    response = client.get(
        "{base}/remote".format(base=MEDIA_BASE_URL),
        params={"url": "https:///video.mp4"},
    )
    assert response.status_code == 400


def test_remote_stream_blocks_private_hosts(client):
    response = client.get(
        "{base}/remote".format(base=MEDIA_BASE_URL),
        params={"url": "http://127.0.0.1/private.mp4"},
    )
    assert response.status_code == 400
