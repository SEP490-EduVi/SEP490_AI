"""Central configuration from the root .env file."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root (one level up from this service folder)
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path, override=False)


class Config:
    """Environment-based configuration."""

    # Google Cloud
    GCS_BUCKET_NAME: str = os.getenv("GCS_BUCKET_NAME", "")
    GOOGLE_APPLICATION_CREDENTIALS: str = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS", ""
    )

    # Neo4j
    NEO4J_URI: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "")

    # Vertex AI (Gemini)
    GOOGLE_CLOUD_PROJECT: str = os.getenv("GOOGLE_CLOUD_PROJECT", "")
    VERTEX_AI_LOCATION: str = os.getenv("VERTEX_AI_LOCATION", "us-central1")

    # RabbitMQ
    RABBITMQ_HOST: str = os.getenv("RABBITMQ_HOST", "localhost")
    RABBITMQ_PORT: int = int(os.getenv("RABBITMQ_PORT", "5672"))
    RABBITMQ_USER: str = os.getenv("RABBITMQ_USER", "guest")
    RABBITMQ_PASSWORD: str = os.getenv("RABBITMQ_PASSWORD", "guest")
    RABBITMQ_VIRTUAL_HOST: str = os.getenv("RABBITMQ_VIRTUAL_HOST", "/")

    # Queue names
    REQUEST_QUEUE: str = "textbook.ingestion.requests"
    DELETION_QUEUE: str = "textbook.deletion.requests"
    RESULT_QUEUE: str = "textbook.ingestion.results"

    # Only 1 ingestion at a time (Gemini calls are heavy, no need for parallelism)
    PREFETCH_COUNT: int = int(os.getenv("TEXTBOOK_PREFETCH_COUNT", "1"))

    # Pipeline settings
    CHAPTER_LIMIT: str = os.getenv("CHAPTER_LIMIT", "ALL").strip().upper()

    # Temp directory for GCS downloads
    TEMP_DIR: str = os.path.join(os.path.dirname(__file__), "temp_downloads")

    @classmethod
    def validate(cls) -> None:
        """Ensure required config values are present."""
        missing: list[str] = []

        if not cls.GCS_BUCKET_NAME:
            missing.append("GCS_BUCKET_NAME")
        if not cls.GOOGLE_CLOUD_PROJECT:
            missing.append("GOOGLE_CLOUD_PROJECT")
        if not cls.NEO4J_PASSWORD:
            missing.append("NEO4J_PASSWORD")

        if missing:
            raise ValueError(
                "Missing required environment variables: %s" % ", ".join(missing)
            )

        os.makedirs(cls.TEMP_DIR, exist_ok=True)
