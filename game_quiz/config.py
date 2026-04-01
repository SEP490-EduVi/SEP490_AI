"""Configuration for game_quiz worker."""

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

    REQUEST_QUEUE: str = os.getenv("GAME_QUIZ_REQUEST_QUEUE", "game.quiz.requests")
    RESULT_QUEUE: str = os.getenv("GAME_QUIZ_RESULT_QUEUE", "game.quiz.results")
    PREFETCH_COUNT: int = int(os.getenv("GAME_QUIZ_PREFETCH_COUNT", "2"))

    PERSIST_PROCESSING_EVENTS: bool = (
        os.getenv("GAME_QUIZ_PERSIST_PROCESSING_EVENTS", "false").strip().lower() == "true"
    )
    TASK_IDEMPOTENCY_ENABLED: bool = (
        os.getenv("GAME_QUIZ_TASK_IDEMPOTENCY_ENABLED", "true").strip().lower() == "true"
    )
    TASK_IDEMPOTENCY_WINDOW_SEC: float = float(
        os.getenv("GAME_QUIZ_TASK_IDEMPOTENCY_WINDOW_SEC", "120")
    )

    GOOGLE_CLOUD_PROJECT: str = os.getenv("GOOGLE_CLOUD_PROJECT", "")
    VERTEX_AI_LOCATION: str = os.getenv("VERTEX_AI_LOCATION", "us-central1")
    GEMINI_MODEL: str = os.getenv("GAME_QUIZ_GEMINI_MODEL", "gemini-2.5-flash")
    HELICONE_API_KEY: str = os.getenv("HELICONE_API_KEY", "")

    QUIZ_DEFAULT_OPTION_COUNT: int = int(os.getenv("QUIZ_DEFAULT_OPTION_COUNT", "4"))
    DEFAULT_ROUND_COUNT: int = int(os.getenv("GAME_QUIZ_DEFAULT_ROUND_COUNT", "1"))
    MAX_ROUND_COUNT: int = int(os.getenv("GAME_QUIZ_MAX_ROUND_COUNT", "20"))
    MAX_SOURCE_CHARS: int = int(os.getenv("GAME_QUIZ_MAX_SOURCE_CHARS", "12000"))
    MAX_TEMPLATE_CHARS: int = int(os.getenv("GAME_QUIZ_MAX_TEMPLATE_CHARS", "8000"))
    JSON_RETRY_COUNT: int = int(os.getenv("GAME_QUIZ_JSON_RETRY_COUNT", "1"))
    LLM_TIMEOUT_SEC: float = float(os.getenv("GAME_QUIZ_LLM_TIMEOUT_SEC", "120"))
    LOG_RESULT_PAYLOAD: bool = (
        os.getenv("GAME_QUIZ_LOG_RESULT_PAYLOAD", "false").strip().lower() == "true"
    )
    LOG_RESULT_MAX_CHARS: int = int(os.getenv("GAME_QUIZ_LOG_RESULT_MAX_CHARS", "4000"))

    @classmethod
    def validate(cls) -> None:
        if not cls.GOOGLE_CLOUD_PROJECT:
            raise ValueError("Missing required environment variable: GOOGLE_CLOUD_PROJECT")
