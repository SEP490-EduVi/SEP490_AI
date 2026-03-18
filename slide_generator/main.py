"""
Slide Generator Service — Async RabbitMQ Worker
=================================================
Listens on the 'slide.generation.requests' queue for tasks from the
ASP.NET backend. Processes each request (evaluation result + preferences)
and publishes progress updates + the final IDocument cards to 'pipeline.results'.

The ASP.NET BackgroundService consumes the result and:
  1. Wraps the cards into a full IDocument (adds id, createdAt, updatedAt)
  2. Stores in Product.SlideDocument column
  3. Pushes to frontend via SignalR

Uses aio-pika (async) so a single container can handle many concurrent tasks.

Local testing (without RabbitMQ):
    python main.py --cli <path_to_test_input.json>

test_input.json format:
{
  "evaluationResult": { "matched_lesson": {"id": "dia_li_lop_10_L1"}, "evaluation": {}, "curriculum": {} },
  "lessonPlanText": "...",
  "preferences": {"slideRange": "medium"}
}
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
    """Handle an incoming slide generation task from RabbitMQ."""
    msg = {}
    async with message.process():
        try:
            msg = json.loads(message.body)
            task_id = msg["taskId"]
            user_id = msg["userId"]
            product_id = msg.get("productId")

            evaluation_result = msg.get("evaluationResult", {})
            lesson_plan_text = msg.get("lessonPlanText", "")
            preferences = msg.get("preferences", {})

            logger.info(
                "Received task %s | slideRange=%s | lesson=%s",
                task_id,
                preferences.get("slideRange", "medium"),
                evaluation_result.get("matched_lesson", {}).get("id", "unknown"),
            )

            # Publish "started"
            await publish_progress(
                channel, task_id, user_id, product_id,
                "processing", "started", 0,
                detail="Task received, starting slide generation",
            )

            # Progress callback → publishes to RabbitMQ
            async def on_progress(step: str, progress: int, detail: str = "") -> None:
                await publish_progress(
                    channel, task_id, user_id, product_id,
                    "processing", step, progress, detail=detail,
                )

            # Run the pipeline
            result = await run(
                evaluation_result=evaluation_result,
                lesson_plan_text=lesson_plan_text,
                preferences=preferences,
                on_progress=on_progress,
            )

            # Publish completed with the IDocument data
            await publish_progress(
                channel, task_id, user_id, product_id,
                "completed", "slides_completed", 100,
                result=result,
            )

            logger.info("Task %s completed successfully (%d cards).", task_id, len(result.get("cards", [])))

        except Exception as exc:
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


# ── CLI Mode (for local testing without RabbitMQ) ─────


def cli_mode() -> None:
    """Run the pipeline once from a JSON input file (no RabbitMQ)."""
    if len(sys.argv) < 3:
        print("Usage: python main.py --cli <path_to_test_input.json>")
        print("\ntest_input.json format:")
        print(json.dumps({
            "evaluationResult": {"matched_lesson": {"id": "dia_li_lop_10_L1"}, "subject": "dia_li", "grade": "10", "evaluation": {}, "curriculum": {}},
            "lessonPlanText": "...",
            "preferences": {"slideRange": "medium"},
        }, ensure_ascii=False, indent=2))
        sys.exit(1)

    input_path = sys.argv[2]
    with open(input_path, encoding="utf-8") as f:
        data = json.load(f)

    result = asyncio.run(run(
        evaluation_result=data.get("evaluationResult", {}),
        lesson_plan_text=data.get("lessonPlanText", ""),
        preferences=data.get("preferences", {}),
    ))

    print("\n" + "=" * 60)
    print("SLIDE GENERATION RESULT")
    print("=" * 60)
    print(f"Title: {result['title']}")
    print(f"Cards: {len(result['cards'])}")
    print("=" * 60)
    print(json.dumps(result, indent=2, ensure_ascii=False))


# ── Entry point ───────────────────────────────────────────────────────


if __name__ == "__main__":
    Config.validate()

    if len(sys.argv) >= 2 and sys.argv[1] == "--cli":
        cli_mode()
    else:
        asyncio.run(start_consumer())
