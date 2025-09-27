from functools import lru_cache
from pathlib import Path
from typing import List

from pydantic import Field, validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "LamAPI"
    app_version: str = "1.0.0"
    description_file: Path = Field(Path(__file__).resolve().parent.parent / "data.txt")

    queue_max_size: int = Field(200, env="LAMAPI_QUEUE_SIZE")
    queue_workers: int = Field(4, env="LAMAPI_WORKERS")
    queue_retries: int = Field(3, env="LAMAPI_JOB_RETRIES")
    queue_backoff: float = Field(0.5, env="LAMAPI_RETRY_BACKOFF")

    cors_origins: List[str] = Field(default_factory=lambda: ["*"])

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    @validator("cors_origins", pre=True)
    def _split_origins(cls, value):  # noqa: N805
        if isinstance(value, str):
            return [origin.strip() for origin in value.split(",") if origin.strip()]
        return value

    @property
    def description(self) -> str:
        try:
            return self.description_file.read_text(encoding="utf-8")
        except FileNotFoundError:
            return "LamAPI"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
