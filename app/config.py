from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # PostgreSQL
    postgres_user: str = "webhook_engine"
    postgres_password: str = "webhook_engine_password"
    postgres_db: str = "webhook_engine"
    postgres_host: str = "postgres"
    postgres_port: int = 5432

    # Redis
    redis_url: str = "redis://redis:6379/0"

    # Worker / retry tuning
    worker_concurrency: int = 50
    max_retry_attempts: int = 5
    base_retry_delay_seconds: float = 2.0
    max_retry_delay_seconds: float = 60.0
    http_timeout_seconds: float = 3.0

    # Broker topology
    stream_name: str = "webhooks:events:stream"
    consumer_group: str = "engine_workers_group"
    retry_zset_key: str = "webhooks:retry:zset"
    scheduler_poll_interval_seconds: float = 0.5
    stream_batch_size: int = 10
    stream_block_ms: int = 2000

    # App
    app_env: str = "development"
    log_level: str = "INFO"

    @property
    def database_url(self) -> str:
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
