"""
Lesson Analysis Service — RabbitMQ Worker
==========================================
Listens on the 'lesson.analysis.requests' queue for tasks from the
ASP.NET backend. Processes each lesson plan and publishes progress
updates + final results to the 'pipeline.results' queue.

The ASP.NET BackgroundService consumes the results and pushes them
to the frontend via SignalR.

Local testing (without RabbitMQ):
    python main.py --cli <gcs_uri> <subject> <grade> [lesson_id]
"""

import json
import logging
import sys
import io
import traceback
import time
import pika
from config import Config
from pipeline import run
from rabbitmq_utils import get_connection, declare_queues, publish_progress

# Fix Windows console encoding for Vietnamese text
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ── RabbitMQ Consumer Mode ────────────────────────────────────────────


def _on_message(channel, method, _properties, body):
    """Handle an incoming task message from RabbitMQ."""
    message = {}
    try:
        message = json.loads(body)
        task_id = message["taskId"]
        user_id = message["userId"]
        product_id = message.get("productId")
        gcs_uri = message["gcsUri"]
        subject = message.get("subjectCode", "")
        grade = message.get("gradeCode", "")
        lesson_id = message.get("lessonCode")  # Neo4j lesson ID, e.g. "dia_li_10_bai_1"

        logger.info(
            "Received task %s | lesson=%s | gcs=%s",
            task_id, lesson_id, gcs_uri,
        )

        # Publish "started"
        publish_progress(channel, task_id, user_id, product_id, "processing", "started", 0,
                         detail="Task received, starting pipeline")

        # Build a progress callback that publishes to RabbitMQ
        def on_progress(step: str, progress: int, detail: str = "") -> None:
            publish_progress(channel, task_id, user_id, product_id, "processing", step, progress,
                             detail=detail)

        # Run the pipeline
        result = run(
            gcs_uri=gcs_uri,
            subject=subject,
            grade=grade,
            lesson_id=lesson_id,
            on_progress=on_progress,
        )

        # Publish completed
        publish_progress(channel, task_id, user_id, product_id, "completed", "completed", 100,
                         result=result)

        logger.info("Task %s completed successfully.", task_id)

    except Exception as exc:
        # Publish failure
        error_msg = traceback.format_exc()
        logger.exception("Task %s failed.", message.get("taskId", "unknown"))

        try:
            publish_progress(
                channel,
                message.get("taskId", "unknown"),
                message.get("userId", "unknown"),
                message.get("productId"),
                "failed", "error", 0,
                error=str(exc),
                detail=error_msg,
            )
        except Exception:
            logger.exception("Failed to publish error result.")

    finally:
        # Always acknowledge — failed tasks go to the result queue with status=failed.
        # (Add DLQ later for retryable failures if needed.)
        channel.basic_ack(delivery_tag=method.delivery_tag)


def start_consumer() -> None:
    """Connect to RabbitMQ and start consuming forever."""
    logger.info("Connecting to RabbitMQ at %s:%s ...", Config.RABBITMQ_HOST, Config.RABBITMQ_PORT)

    while True:
        try:
            connection = get_connection()
            channel = connection.channel()
            declare_queues(channel)

            # Process one message at a time
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(
                queue=Config.REQUEST_QUEUE,
                on_message_callback=_on_message,
            )

            logger.info(
                "Connected! Waiting for messages on '%s' ...",
                Config.REQUEST_QUEUE,
            )
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError as exc:
            logger.warning("RabbitMQ connection lost: %s. Reconnecting in 5s...", exc)
            time.sleep(5)
        except KeyboardInterrupt:
            logger.info("Shutting down consumer.")
            break


# ── CLI Mode (for manual / local testing without RabbitMQ) ────────────


def cli_mode() -> None:
    """Run the pipeline once from command-line arguments (no RabbitMQ)."""
    if len(sys.argv) < 4:
        print("Usage: python main.py --cli <gcs_uri> <subject> <grade> [lesson_id]")
        sys.exit(1)

    gcs_uri = sys.argv[2]
    subject = sys.argv[3]
    grade = sys.argv[4]
    lesson_id = sys.argv[5] if len(sys.argv) >= 6 else None

    result = run(gcs_uri, subject, grade, lesson_id=lesson_id)

    print("\n" + "=" * 60)
    print("EVALUATION RESULT")
    print("=" * 60)
    print(json.dumps(result, indent=2, ensure_ascii=False))


# ── Entry point ───────────────────────────────────────────────────────


if __name__ == "__main__":
    Config.validate()

    if len(sys.argv) >= 2 and sys.argv[1] == "--cli":
        cli_mode()
    else:
        start_consumer()
