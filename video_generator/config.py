"""Configuration for video generator worker."""

import os

# ── RabbitMQ Configuration ────────────────────────────────────────────────────
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")

# ── Queue Names ───────────────────────────────────────────────────────────────
REQUEST_QUEUE = "video.generation.requests"
RESULT_QUEUE = "pipeline.results"

# ── Worker Settings ───────────────────────────────────────────────────────────
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "1"))

# Limit slide-level concurrency per job to avoid CPU/RAM spikes.
MAX_SLIDE_CONCURRENCY = int(os.getenv("MAX_SLIDE_CONCURRENCY", "2"))

# Stage-level throttles to stabilize throughput under load.
RENDER_CONCURRENCY = int(os.getenv("RENDER_CONCURRENCY", "1"))
TTS_CONCURRENCY = int(os.getenv("TTS_CONCURRENCY", "2"))
FFMPEG_CONCURRENCY = int(os.getenv("FFMPEG_CONCURRENCY", "1"))
PROBE_CONCURRENCY = int(os.getenv("PROBE_CONCURRENCY", "1"))

# RabbitMQ message durability tuning.
# Keep final completed/failed results persistent, allow processing progress to be transient.
PERSIST_PROCESSING_EVENTS = (
	os.getenv("PERSIST_PROCESSING_EVENTS", "false").strip().lower() == "true"
)

# Retry transient Edge TTS failures like NoAudioReceived.
TTS_MAX_RETRIES = int(os.getenv("TTS_MAX_RETRIES", "3"))
TTS_RETRY_DELAY_SEC = float(os.getenv("TTS_RETRY_DELAY_SEC", "0.8"))

# Keep Playwright browser alive across requests for better throughput.
CLEANUP_BROWSER_EACH_REQUEST = (
	os.getenv("CLEANUP_BROWSER_EACH_REQUEST", "false").strip().lower() == "true"
)

# ── Video Output ──────────────────────────────────────────────────────────────
OUTPUT_DIR = os.getenv("VIDEO_OUTPUT_DIR", "/app/videos")
VIDEO_BASE_URL = os.getenv("VIDEO_BASE_URL", "http://localhost:8000/videos")

# ── GCS Upload (optional, recommended for backend consumption) ───────────────
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "")
VIDEO_GCS_PREFIX = os.getenv("VIDEO_GCS_PREFIX", "generated_videos")

# If true and GCS_BUCKET_NAME is configured, pipeline returns gs:// URI as video_url.
VIDEO_RETURN_GCS_URI = (
	os.getenv("VIDEO_RETURN_GCS_URI", "true").strip().lower() == "true"
)

# If false, cards that look like embedded video must resolve to real video clips,
# otherwise pipeline raises an explicit error instead of rendering screenshot fallback.
VIDEO_SCREENSHOT_FALLBACK = (
	os.getenv("VIDEO_SCREENSHOT_FALLBACK", "false").strip().lower() == "true"
)

# Concat tuning: keep copy path as fast as possible in production.
# Disable faststart on copy path by default to avoid extra remux overhead.
CONCAT_FASTSTART_ON_COPY = (
	os.getenv("CONCAT_FASTSTART_ON_COPY", "false").strip().lower() == "true"
)
