import asyncio
from pathlib import Path
from threading import Event
from typing import Tuple

from app.services.media_service import MediaService

STREAMS_ACTIVE_URL = "/api/v1/streams/active"
SAMPLE_VIDEO_NAME = "tiny.mp4"


def test_active_streams_returns_zero_when_idle(client):
    """
    Verifies the public counter endpoint starts at zero so callers can rely on
    it as a live view of currently iterated streams, not historical requests.
    """

    response = client.get(STREAMS_ACTIVE_URL)

    assert response.status_code == 200
    assert response.json() == {"count": 0}


def test_active_streams_reflects_in_flight_stream_tracking(client):
    """
    Holds the counter open in the app event loop and reads it through HTTP so
    the endpoint proves it observes active streaming state from app state.
    """

    entered = Event()
    release = Event()

    async def hold_stream_slot() -> None:
        async with client.app.state.container.stream_counter.track():
            entered.set()
            await asyncio.to_thread(release.wait)

    future = client.portal.start_task_soon(hold_stream_slot)
    try:
        assert entered.wait(timeout=2)
        response = client.get(STREAMS_ACTIVE_URL)
        assert response.status_code == 200
        assert response.json() == {"count": 1}
    finally:
        release.set()
        future.result(timeout=2)

    response = client.get(STREAMS_ACTIVE_URL)
    assert response.status_code == 200
    assert response.json() == {"count": 0}


def test_media_generator_count_drops_after_stream_closes(client, tmp_path: Path):
    """
    Consumes one media chunk directly so the test can observe that counting
    starts during generator iteration and decrements when the stream closes.
    """

    samples_dir = tmp_path / "media"
    samples_dir.mkdir()
    sample_path = samples_dir / SAMPLE_VIDEO_NAME
    sample_path.write_bytes(bytes(range(256)) * 16)

    client.app.state.container.settings.media_samples_dir = str(samples_dir)

    async def consume_and_close() -> Tuple[int, int, int]:
        service = MediaService(state=client.app.state.container)
        generator = service._stream_full_file(path=sample_path)
        chunk = await generator.__anext__()
        active_count = await client.app.state.container.stream_counter.get()
        await generator.aclose()
        closed_count = await client.app.state.container.stream_counter.get()
        return len(chunk), active_count, closed_count

    chunk_size, active_count, closed_count = client.portal.call(consume_and_close)

    assert chunk_size > 0
    assert active_count == 1
    assert closed_count == 0
