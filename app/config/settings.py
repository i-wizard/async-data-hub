from functools import lru_cache
from typing import Annotated, List, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


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
    http_timeout_seconds: float = Field(default=50.0, alias="HTTP_TIMEOUT_SECONDSs")
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
    frontend_origins: Annotated[List[str], NoDecode] = Field(
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
    media_samples_dir: str = Field(default="./sample_media", alias="MEDIA_SAMPLES_DIR")
    media_chunk_size: int = Field(default=64 * 1024, alias="MEDIA_CHUNK_SIZE") # 64KB
    remote_media_allowed_schemes: Annotated[List[str], NoDecode] = Field(
        default=["http", "https"],
        alias="REMOTE_MEDIA_ALLOWED_SCHEMES",
    )
    remote_media_allowed_hosts: Annotated[List[str], NoDecode] = Field(
        default=[],
        alias="REMOTE_MEDIA_ALLOWED_HOSTS",
    )
    remote_media_block_private_hosts: bool = Field(
        default=True,
        alias="REMOTE_MEDIA_BLOCK_PRIVATE_HOSTS",
    )
    remote_media_chunk_size: int = Field(
        default=64 * 1024,
        alias="REMOTE_MEDIA_CHUNK_SIZE",
    )

    # REPLICATION CONFIG
    database_replica_url: Optional[str] = Field(
        default=None, alias="DATABASE_URL_REPLICA1"
    )
    database_replica_url2: Optional[str] = Field(
        default=None, alias="DATABASE_URL_REPLICA2"
    )
    database_admin_url: str = Field(
        default="postgresql+asyncpg://postgres:adminpass@localhost:5440/consistency",
        alias="DATABASE_URL_ADMIN",
    )
    replication_availability: bool = Field(default=False, alias="REPLICATION_AVAILABILITY")


    @field_validator(
        "frontend_origins",
        "remote_media_allowed_schemes",
        "remote_media_allowed_hosts",
        mode="before",
    )
    @classmethod
    def parse_csv_list(cls, value):
        """
        Accepts either a list or comma-separated env var so local development
        settings stay readable without requiring JSON syntax in .env files.
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
