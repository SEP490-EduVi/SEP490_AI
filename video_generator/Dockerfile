# ── Video Generator Worker ───────────────────────────────────────────────────────
FROM python:3.12-slim

# Prevent Python from buffering stdout/stderr
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Install ffmpeg + fonts (DejaVu for Vietnamese support)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        fonts-dejavu-core \
        fonts-dejavu-extra && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies (cached layer)
COPY video_generator/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browser runtime used by slide renderer.
RUN playwright install --with-deps chromium

# Copy application code
COPY video_generator/ ./video_generator/

# Create output directory
RUN mkdir -p /app/videos

# Environment variables (can be overridden in docker-compose)
ENV RABBITMQ_HOST=rabbitmq \
    RABBITMQ_PORT=5672 \
    RABBITMQ_USER=guest \
    RABBITMQ_PASS=guest \
    VIDEO_OUTPUT_DIR=/app/videos \
    VIDEO_BASE_URL=http://video-generator:8000/videos

# Run the RabbitMQ consumer
CMD ["python", "-m", "video_generator.main"]
