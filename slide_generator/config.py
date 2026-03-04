"""Central configuration — loads from the shared root .env file."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root (one level up from this service folder).
# In Docker: env vars are injected via docker-compose, this is a no-op.
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path, override=False)


class Config:
    """All configuration values, sourced from environment variables."""

    # ── Vertex AI (Gemini) ─────────────────────────────────────────────
    GOOGLE_CLOUD_PROJECT: str = os.getenv("GOOGLE_CLOUD_PROJECT", "")
    VERTEX_AI_LOCATION: str = os.getenv("VERTEX_AI_LOCATION", "us-central1")
    GOOGLE_APPLICATION_CREDENTIALS: str = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

    # ── RabbitMQ ───────────────────────────────────────────────────────
    RABBITMQ_HOST: str = os.getenv("RABBITMQ_HOST", "localhost")
    RABBITMQ_PORT: int = int(os.getenv("RABBITMQ_PORT", "5672"))
    RABBITMQ_USER: str = os.getenv("RABBITMQ_USER", "guest")
    RABBITMQ_PASSWORD: str = os.getenv("RABBITMQ_PASSWORD", "guest")
    RABBITMQ_VIRTUAL_HOST: str = os.getenv("RABBITMQ_VIRTUAL_HOST", "/")

    # ── Queue names ────────────────────────────────────────────────────
    REQUEST_QUEUE: str = "slide.generation.requests"
    RESULT_QUEUE: str = "pipeline.results"

    # ── Gemini model ───────────────────────────────────────────────────
    GEMINI_MODEL: str = "gemini-2.5-flash"

    # ── Slide range limits ─────────────────────────────────────────────
    SLIDE_RANGE_MAP: dict = {
        "short":    (5, 8),
        "medium":   (10, 15),
        "detailed": (15, 20),
    }
    DEFAULT_SLIDE_RANGE: str = "medium"

    @classmethod
    def validate(cls) -> None:
        """Ensure required config values are present."""
        missing: list[str] = []

        if not cls.GOOGLE_CLOUD_PROJECT:
            missing.append("GOOGLE_CLOUD_PROJECT")

        if missing:
            raise ValueError(
                "Missing required environment variables: %s" % ", ".join(missing)
            )
