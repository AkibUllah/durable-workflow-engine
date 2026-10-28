from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    database_url: str = "sqlite:///./durable.db"
    api_key: str | None = None  # when set, /v1/* requires X-API-Key
    log_level: str = "INFO"

    # Run worker threads inside the API process (handy for one-container deploys).
    embedded_worker: bool = True
    worker_concurrency: int = 2
    lease_seconds: float = 30.0
    poll_interval: float = 0.5


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
