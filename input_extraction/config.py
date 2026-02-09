import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Central configuration loaded from environment variables."""

    # Google Cloud Storage
    GCS_BUCKET_NAME: str = os.getenv("GCS_BUCKET_NAME", "")
    GOOGLE_APPLICATION_CREDENTIALS: str = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

    # RabbitMQ (for future use)
    RABBITMQ_HOST: str = os.getenv("RABBITMQ_HOST", "localhost")
    RABBITMQ_PORT: int = int(os.getenv("RABBITMQ_PORT", "5672"))
    RABBITMQ_USER: str = os.getenv("RABBITMQ_USER", "guest")
    RABBITMQ_PASSWORD: str = os.getenv("RABBITMQ_PASSWORD", "guest")
    RABBITMQ_QUEUE: str = os.getenv("RABBITMQ_QUEUE", "file_extraction_queue")

    # Local temp directory for downloaded files
    TEMP_DIR: str = os.path.join(os.path.dirname(__file__), "temp_downloads")

    @classmethod
    def validate(cls) -> None:
        """Ensure required config values are present."""
        if not cls.GCS_BUCKET_NAME:
            raise ValueError("GCS_BUCKET_NAME is not set in environment variables.")
        os.makedirs(cls.TEMP_DIR, exist_ok=True)
