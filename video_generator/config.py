"""Configuration for video_generator service."""

import os

# RabbitMQ
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
REQUEST_QUEUE = os.getenv("VIDEO_REQUEST_QUEUE", "video.generation.requests")
RESULT_QUEUE = os.getenv("VIDEO_RESULT_QUEUE", "pipeline.results")
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "3"))

# Progress event behavior
PERSIST_PROCESSING_EVENTS = (
    os.getenv("PERSIST_PROCESSING_EVENTS", "false").strip().lower() == "true"
)

# Duplicate task protection
TASK_IDEMPOTENCY_ENABLED = (
    os.getenv("TASK_IDEMPOTENCY_ENABLED", "true").strip().lower() == "true"
)
TASK_IDEMPOTENCY_WINDOW_SEC = float(os.getenv("TASK_IDEMPOTENCY_WINDOW_SEC", "120"))

# Pipeline concurrency
MAX_SLIDE_CONCURRENCY = int(os.getenv("MAX_SLIDE_CONCURRENCY", "1"))
RENDER_CONCURRENCY = int(os.getenv("RENDER_CONCURRENCY", "1"))
TTS_CONCURRENCY = int(os.getenv("TTS_CONCURRENCY", "1"))
FFMPEG_CONCURRENCY = int(os.getenv("FFMPEG_CONCURRENCY", "1"))
PROBE_CONCURRENCY = int(os.getenv("PROBE_CONCURRENCY", "1"))
FFMPEG_THREADS = int(os.getenv("FFMPEG_THREADS", "1"))
FFMPEG_PRESET = os.getenv("FFMPEG_PRESET", "ultrafast")
FFMPEG_TIMEOUT_SEC = int(os.getenv("FFMPEG_TIMEOUT_SEC", "120"))
VIDEO_FPS = int(os.getenv("VIDEO_FPS", "24"))
VIDEO_WIDTH = int(os.getenv("VIDEO_WIDTH", "1280"))
VIDEO_HEIGHT = int(os.getenv("VIDEO_HEIGHT", "720"))
VIDEO_TRACK_TIMESCALE = int(os.getenv("VIDEO_TRACK_TIMESCALE", "90000"))

# Audio encoding knobs (lower defaults reduce CPU and output size).
AUDIO_CHANNELS = int(os.getenv("AUDIO_CHANNELS", "1"))
AUDIO_SAMPLE_RATE = int(os.getenv("AUDIO_SAMPLE_RATE", "24000"))
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "64k")

# Fastest/lowest-CPU concat mode is stream-copy.
# Keep fallback disabled by default; enable only when copy fails on mixed assets.
CONCAT_ALLOW_REENCODE_FALLBACK = (
    os.getenv("CONCAT_ALLOW_REENCODE_FALLBACK", "false").strip().lower() == "true"
)

# Reduce event spam to RabbitMQ during per-card processing.
PROGRESS_STEP_GRANULARITY = int(os.getenv("PROGRESS_STEP_GRANULARITY", "10"))

# Media output
OUTPUT_DIR = os.getenv("VIDEO_OUTPUT_DIR", "/app/videos")
VIDEO_BASE_URL = os.getenv("VIDEO_BASE_URL", "http://localhost:8000/videos")

# Optional GCS upload
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "")
VIDEO_GCS_PREFIX = os.getenv("VIDEO_GCS_PREFIX", "generated_videos")
VIDEO_RETURN_GCS_URI = (
    os.getenv("VIDEO_RETURN_GCS_URI", "true").strip().lower() == "true"
)

# Runtime cleanup
# Keep browser alive across requests — avoids ~1-2s Chromium cold-start per job.
CLEANUP_BROWSER_EACH_REQUEST = (
    os.getenv("CLEANUP_BROWSER_EACH_REQUEST", "false").strip().lower() == "true"
)

# TTS retry
TTS_MAX_RETRIES = int(os.getenv("TTS_MAX_RETRIES", "3"))
TTS_RETRY_DELAY_SEC = float(os.getenv("TTS_RETRY_DELAY_SEC", "0.8"))

# Interaction timing
INTERACTION_CLIP_DURATION_SEC = float(os.getenv("INTERACTION_CLIP_DURATION_SEC", "0.2"))
QUIZ_PAUSE_OFFSET_SEC = float(os.getenv("QUIZ_PAUSE_OFFSET_SEC", "0.1"))
QUIZ_PAUSE_EDGE_GUARD_SEC = float(os.getenv("QUIZ_PAUSE_EDGE_GUARD_SEC", "0.01"))

