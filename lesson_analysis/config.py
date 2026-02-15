"""Central configuration – loads from the shared root .env file."""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root (one level up from this service folder)
# In Docker: env vars are injected via docker-compose, this is a no-op.
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path, override=False)


class Config:
    """All configuration values, sourced from environment variables."""

    # ── Google Cloud Storage ──────────────────────────────────────────
    GCS_BUCKET_NAME: str = os.getenv("GCS_BUCKET_NAME", "")
    GOOGLE_APPLICATION_CREDENTIALS: str = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

    # ── Neo4j ─────────────────────────────────────────────────────────
    NEO4J_URI: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")
    NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "")

    # ── Gemini LLM ────────────────────────────────────────────────────
    GEMINI_API_KEY: str = os.getenv("GEMINI_API_KEY", "")

    # ── Temp directory for downloaded files ────────────────────────────
    TEMP_DIR: str = os.path.join(os.path.dirname(__file__), "temp_downloads")

    @classmethod
    def validate(cls) -> None:
        """Ensure required config values are present."""
        missing: list[str] = []

        if not cls.GCS_BUCKET_NAME:
            missing.append("GCS_BUCKET_NAME")
        if not cls.GEMINI_API_KEY:
            missing.append("GEMINI_API_KEY")
        if not cls.NEO4J_PASSWORD:
            missing.append("NEO4J_PASSWORD")

        if missing:
            raise ValueError(
                "Missing required environment variables: %s" % ", ".join(missing)
            )

        os.makedirs(cls.TEMP_DIR, exist_ok=True)
