"""Configuration for ai_review worker."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv


env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path, override=False)


class Config:
    """Configuration values sourced from environment variables."""

    RABBITMQ_HOST: str = os.getenv("RABBITMQ_HOST", "localhost")
    RABBITMQ_PORT: int = int(os.getenv("RABBITMQ_PORT", "5672"))
    RABBITMQ_USER: str = os.getenv("RABBITMQ_USER", "guest")
    RABBITMQ_PASS: str = os.getenv("RABBITMQ_PASS", os.getenv("RABBITMQ_PASSWORD", "guest"))
    RABBITMQ_VHOST: str = os.getenv("RABBITMQ_VHOST", os.getenv("RABBITMQ_VIRTUAL_HOST", "/"))

    REQUEST_QUEUE: str = os.getenv("FILE_REVIEW_REQUEST_QUEUE", "file.review.requests")
    RESULT_QUEUE: str = os.getenv("FILE_REVIEW_RESULT_QUEUE", "file.review.results")
    PREFETCH_COUNT: int = int(os.getenv("FILE_REVIEW_PREFETCH_COUNT", "3"))

    GCS_BUCKET_NAME: str = os.getenv("GCS_BUCKET_NAME", "")
    GOOGLE_CLOUD_PROJECT: str = os.getenv("GOOGLE_CLOUD_PROJECT", "")
    GOOGLE_APPLICATION_CREDENTIALS: str = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")
    VERTEX_AI_LOCATION: str = os.getenv("VERTEX_AI_LOCATION", "us-central1")
    GEMINI_MODEL: str = os.getenv("FILE_REVIEW_GEMINI_MODEL", "gemini-2.5-flash")
    HELICONE_API_KEY: str = os.getenv("HELICONE_API_KEY", "")
    MAX_EVIDENCE_CHARS: int = int(os.getenv("FILE_REVIEW_MAX_EVIDENCE_CHARS", "10000"))
    AI_TIMEOUT_SEC: float = float(os.getenv("FILE_REVIEW_AI_TIMEOUT_SEC", "45"))
    AI_RETRY_COUNT: int = int(os.getenv("FILE_REVIEW_AI_RETRY_COUNT", "2"))
    AI_RETRY_BACKOFF_SEC: float = float(os.getenv("FILE_REVIEW_AI_RETRY_BACKOFF_SEC", "2"))

    TEMP_DIR: str = os.path.join(os.path.dirname(__file__), "temp_downloads")

    @classmethod
    def validate(cls) -> None:
        if not cls.GOOGLE_CLOUD_PROJECT:
            raise ValueError("Missing required environment variable: GOOGLE_CLOUD_PROJECT")
        os.makedirs(cls.TEMP_DIR, exist_ok=True)
