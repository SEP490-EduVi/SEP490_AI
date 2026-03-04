"""
Slide Generator Service — RabbitMQ Worker
==========================================
Listens on the 'slide.generation.requests' queue for tasks from the
ASP.NET backend. Processes each request (evaluation result + preferences)
and publishes progress updates + the final IDocument cards to 'pipeline.results'.

The ASP.NET BackgroundService consumes the result and:
  1. Wraps the cards into a full IDocument (adds id, createdAt, updatedAt)
  2. Stores in Product.SlideDocument column
  3. Pushes to frontend via SignalR

Local testing (without RabbitMQ):
    python main.py --cli <path_to_test_input.json>

test_input.json format:
{
  "evaluationResult": { ... },
  "lessonPlanText": "...",
  "textbookSections": [{"heading": "...", "content": "..."}],
  "preferences": {"slideRange": "medium"}
}
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
    """Handle an incoming slide generation task from RabbitMQ."""
    message = {}
    try:
        message = json.loads(body)
        task_id = message["taskId"]
        user_id = message["userId"]
        product_id = message.get("productId")

        evaluation_result = message.get("evaluationResult", {})
        lesson_plan_text = message.get("lessonPlanText", "")
        textbook_sections = message.get("textbookSections", [])
        preferences = message.get("preferences", {})

        logger.info(
            "Received task %s | slideRange=%s | lesson=%s",
            task_id,
            preferences.get("slideRange", "medium"),
            evaluation_result.get("matched_lesson", {}).get("id", "unknown"),
        )

        # Publish "started"
        publish_progress(
            channel, task_id, user_id, product_id,
            "processing", "started", 0,
            detail="Task received, starting slide generation",
        )

        # Progress callback → publishes to RabbitMQ
        def on_progress(step: str, progress: int, detail: str = "") -> None:
            publish_progress(
                channel, task_id, user_id, product_id,
                "processing", step, progress, detail=detail,
            )

        # Run the pipeline
        result = run(
            evaluation_result=evaluation_result,
            lesson_plan_text=lesson_plan_text,
            textbook_sections=textbook_sections,
            preferences=preferences,
            on_progress=on_progress,
        )

        # Publish completed with the IDocument data
        publish_progress(
            channel, task_id, user_id, product_id,
            "completed", "slides_completed", 100,
            result=result,
        )

        logger.info("Task %s completed successfully (%d cards).", task_id, len(result.get("cards", [])))

    except Exception as exc:
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
        channel.basic_ack(delivery_tag=method.delivery_tag)


def start_consumer() -> None:
    """Connect to RabbitMQ and start consuming forever."""
    logger.info(
        "Connecting to RabbitMQ at %s:%s ...",
        Config.RABBITMQ_HOST, Config.RABBITMQ_PORT,
    )

    while True:
        try:
            connection = get_connection()
            channel = connection.channel()
            declare_queues(channel)

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


# ── CLI Mode (for local testing without RabbitMQ) ─────────────────────


def cli_mode() -> None:
    """Run the pipeline once from a JSON input file (no RabbitMQ)."""
    if len(sys.argv) < 3:
        print("Usage: python main.py --cli <path_to_test_input.json>")
        print("\ntest_input.json format:")
        print(json.dumps({
            "evaluationResult": {"subject": "dia_li", "grade": "10", "evaluation": {}},
            "lessonPlanText": "...",
            "textbookSections": [{"heading": "...", "content": "..."}],
            "preferences": {"slideRange": "medium"},
        }, ensure_ascii=False, indent=2))
        sys.exit(1)

    input_path = sys.argv[2]
    with open(input_path, encoding="utf-8") as f:
        data = json.load(f)

    result = run(
        evaluation_result=data.get("evaluationResult", {}),
        lesson_plan_text=data.get("lessonPlanText", ""),
        textbook_sections=data.get("textbookSections", []),
        preferences=data.get("preferences", {}),
    )

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
        start_consumer()
