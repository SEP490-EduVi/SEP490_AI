"""
Lesson Analysis Service — Async RabbitMQ Worker
==========================================
Listens on the 'lesson.analysis.requests' queue for tasks from the
ASP.NET backend. Processes each lesson plan and publishes progress
updates + final results to the 'pipeline.results' queue.

The ASP.NET BackgroundService consumes the results and pushes them
to the frontend via SignalR.

Uses aio-pika (async) so a single container can handle many concurrent tasks.

Local testing (without RabbitMQ):
    python main.py --cli <gcs_uri> <subject> <grade> [lesson_id]
"""

import asyncio
import json
import logging
import sys
import io
import traceback
import aio_pika
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


async def _on_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    channel: aio_pika.abc.AbstractChannel,
) -> None:
    """Handle an incoming task message from RabbitMQ."""
    msg = {}
    async with message.process():
        try:
            msg = json.loads(message.body)
            task_id = msg["taskId"]
            user_id = msg["userId"]
            product_id = msg.get("productId")
            gcs_uri = msg["gcsUri"]
            subject = msg.get("subjectCode", "")
            grade = msg.get("gradeCode", "")
            lesson_id = msg.get("lessonCode")
            curriculum_year = msg.get("curriculumYear")

            if not lesson_id:
                raise ValueError("Missing required field 'lessonCode' in message")

            logger.info(
                "Received task %s | lesson=%s | gcs=%s",
                task_id, lesson_id, gcs_uri,
            )

            # Publish "started"
            await publish_progress(
                channel, task_id, user_id, product_id,
                "processing", "started", 0,
                detail="Task received, starting pipeline",
            )

            # Build an async progress callback that publishes to RabbitMQ
            async def on_progress(step: str, progress: int, detail: str = "") -> None:
                await publish_progress(
                    channel, task_id, user_id, product_id,
                    "processing", step, progress, detail=detail,
                )

            # Run the pipeline
            result = await run(
                gcs_uri=gcs_uri,
                subject=subject,
                grade=grade,
                lesson_id=lesson_id,
                curriculum_year=curriculum_year,
                on_progress=on_progress,
            )

            # Publish completed
            await publish_progress(
                channel, task_id, user_id, product_id,
                "completed", "completed", 100,
                result=result,
            )

            logger.info("Task %s completed successfully.", task_id)

        except Exception as exc:
            # Publish failure
            error_msg = traceback.format_exc()
            logger.exception("Task %s failed.", msg.get("taskId", "unknown"))

            try:
                await publish_progress(
                    channel,
                    msg.get("taskId", "unknown"),
                    msg.get("userId", "unknown"),
                    msg.get("productId"),
                    "failed", "error", 0,
                    error=f"{type(exc).__name__}: {exc}",
                    detail=error_msg,
                )
            except Exception:
                logger.exception("Failed to publish error result.")


async def start_consumer() -> None:
    """Connect to RabbitMQ and start consuming forever (async)."""
    logger.info(
        "Connecting to RabbitMQ at %s:%s ...",
        Config.RABBITMQ_HOST, Config.RABBITMQ_PORT,
    )

    connection = await get_connection()
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=Config.PREFETCH_COUNT)
    await declare_queues(channel)

    queue = await channel.declare_queue(Config.REQUEST_QUEUE, durable=True)

    logger.info(
        "Connected! Waiting for messages on '%s' (prefetch=%d) ...",
        Config.REQUEST_QUEUE, Config.PREFETCH_COUNT,
    )

    await queue.consume(lambda msg: _on_message(msg, channel))

    # Run forever (aio_pika.connect_robust handles reconnections automatically)
    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        logger.info("Shutting down consumer.")
        await connection.close()


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

    result = asyncio.run(run(gcs_uri, subject, grade, lesson_id=lesson_id))

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
        asyncio.run(start_consumer())
