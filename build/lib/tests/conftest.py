from pathlib import Path
from typing import Iterator

import httpx
import pytest
from fastapi.testclient import TestClient

from app.config.settings import get_settings
from app.main import create_app


@pytest.fixture
def client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[TestClient]:
    """
    Creates a fully started application with isolated SQLite and in-memory cache
    settings so tests exercise real async wiring without external services.
    """

    database_path = tmp_path / "test_async_data_hub.db"
    monkeypatch.setenv("DATABASE_URL", "sqlite+aiosqlite:///{path}".format(path=database_path))
    monkeypatch.setenv("REDIS_URL", "memory://")
    monkeypatch.setenv("ENABLE_IN_MEMORY_CACHE_FALLBACK", "true")
    monkeypatch.setenv("PROCESS_POOL_WORKERS", "0")
    monkeypatch.setenv("REMOTE_MEDIA_ALLOWED_HOSTS", "testserver")
    get_settings.cache_clear()

    app = create_app()

    with TestClient(app) as test_client:
        internal_http_client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        )
        test_client.app.state.container.http_client = internal_http_client
        test_client.app.state.container.settings.aggregation_source_base_url = "http://testserver/api/v1/mock-sources"
        yield test_client
        test_client.portal.call(internal_http_client.aclose)

    get_settings.cache_clear()
