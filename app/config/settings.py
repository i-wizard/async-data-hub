from functools import lru_cache
from typing import List, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Stores runtime configuration in one typed object so async resources can be
    created consistently during startup and reused across the entire process.
    """

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    app_name: str = Field(default="Async Data Hub", alias="APP_NAME")
    app_env: str = Field(default="development", alias="APP_ENV")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    database_url: str = Field(
        default="sqlite+aiosqlite:///./async_data_hub.db",
        alias="DATABASE_URL",
    )
    redis_url: str = Field(default="memory://", alias="REDIS_URL")
    http_timeout_seconds: float = Field(default=5.0, alias="HTTP_TIMEOUT_SECONDS")
    http_concurrency_limit: int = Field(default=10, alias="HTTP_CONCURRENCY_LIMIT")
    webhook_concurrency_limit: int = Field(default=5, alias="WEBHOOK_CONCURRENCY_LIMIT")
    websocket_queue_size: int = Field(default=20, alias="WEBSOCKET_QUEUE_SIZE")
    cache_ttl_seconds: int = Field(default=30, alias="CACHE_TTL_SECONDS")
    aggregation_source_base_url: str = Field(
        default="http://127.0.0.1:8000/api/v1/mock-sources",
        alias="AGGREGATION_SOURCE_BASE_URL",
    )
    enable_in_memory_cache_fallback: bool = Field(
        default=True,
        alias="ENABLE_IN_MEMORY_CACHE_FALLBACK",
    )
    frontend_origins: List[str] = Field(
        default=[
            "http://127.0.0.1:5500",
            "http://localhost:5500",
            "http://127.0.0.1:8080",
            "http://localhost:8080",
            "http://localhost:63342",
        ],
        alias="FRONTEND_ORIGINS",
    )
    process_pool_workers: Optional[int] = Field(
        default=None, alias="PROCESS_POOL_WORKERS"
    )

    @field_validator("frontend_origins", mode="before")
    @classmethod
    def parse_frontend_origins(cls, value):
        """
        Accepts either a list or a comma-separated env var so local frontend
        origins can be configured simply without requiring JSON syntax.
        """

        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Caches settings so dependencies and startup code all read the same values
    without repeatedly re-parsing the environment.
    """

    return Settings()
