"""
Curriculum Ingestion Service — Async RabbitMQ Worker
=====================================================
Listens on 'curriculum.ingestion.requests' for tasks published by the ASP.NET backend.
Downloads the curriculum .docx from GCS, parses it with Gemini, ingests into Neo4j,
then publishes progress + final stats to 'curriculum.ingestion.results'.

The ASP.NET BackgroundService consumes the results and updates CurriculumDocument.Status.

Local CLI testing (without RabbitMQ):
    python main.py --cli <gcs_uri> <subject_code> <education_level>
    python main.py --cli gs://bucket/dia_li_2018.docx dia_li THPT
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

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ── RabbitMQ Consumer Mode ────────────────────────────────────────────────────

async def _on_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    channel: aio_pika.abc.AbstractChannel,
) -> None:
    """Handle an incoming curriculum ingestion task message."""
    msg: dict = {}
    async with message.process():
        try:
            msg = json.loads(message.body)
            task_id = msg["taskId"]
            document_id = msg["documentId"]
            gcs_uri = msg["gcsUri"]
            subject_code = msg.get("subjectCode", "")
            education_level = msg.get("educationLevel", "THPT")
            curriculum_year = msg.get("curriculumYear")

            logger.info(
                "Received task %s | document=%s | gcs=%s",
                task_id, document_id, gcs_uri,
            )

            async def on_progress(step: str, progress: int, detail: str = "") -> None:
                await publish_progress(
                    channel, task_id, document_id,
                    "processing", step, progress, detail=detail,
                )

            stats = await run(
                gcs_uri=gcs_uri,
                subject_code=subject_code,
                education_level=education_level,
                curriculum_year=curriculum_year,
                on_progress=on_progress,
            )

            await publish_progress(
                channel, task_id, document_id,
                "completed", "completed", 100,
                detail=(
                    f"Ingested {stats['lop_count']} lớp, "
                    f"{stats['chu_de_count']} chủ đề, "
                    f"{stats['yeu_cau_count']} yêu cầu cần đạt, "
                    f"{stats['covers_count']} COVERS links"
                ),
                stats={k: v for k, v in stats.items() if k != "lop_count"},
            )

            logger.info("Task %s completed. Stats: %s", task_id, stats)

        except Exception as exc:
            error_detail = traceback.format_exc()
            logger.exception("Task %s failed.", msg.get("taskId", "unknown"))

            try:
                await publish_progress(
                    channel,
                    msg.get("taskId", "unknown"),
                    msg.get("documentId", "unknown"),
                    "failed", "error", 0,
                    detail=error_detail,
                    error=f"{type(exc).__name__}: {exc}",
                )
            except Exception:
                logger.exception("Failed to publish error result.")


async def start_consumer() -> None:
    """Connect to RabbitMQ and start consuming forever."""
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

    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        logger.info("Shutting down consumer.")
        await connection.close()


# ── CLI Mode ─────────────────────────────────────────────────────────────────

def cli_mode() -> None:
    """Run the full pipeline once from CLI arguments (no RabbitMQ required)."""
    if len(sys.argv) < 5:
        print("Usage: python main.py --cli <gcs_uri> <subject_code> <education_level> [curriculum_year]")
        print("Example: python main.py --cli gs://bucket/dia_li.docx dia_li THPT 2018")
        sys.exit(1)

    gcs_uri = sys.argv[2]
    subject_code = sys.argv[3]
    education_level = sys.argv[4]
    curriculum_year = int(sys.argv[5]) if len(sys.argv) > 5 else None

    stats = asyncio.run(run(gcs_uri, subject_code, education_level, curriculum_year=curriculum_year))

    print("\n" + "=" * 60)
    print("INGESTION COMPLETE")
    print("=" * 60)
    print(json.dumps(stats, indent=2, ensure_ascii=False))


def apply_mapping_mode() -> None:
    """
    Re-apply the saved mapping JSON without re-parsing the document.
    Use after manually editing mapping/{subject_code}_{level_slug}.json.

    Usage: python main.py --apply-mapping <subject_code> <education_level>
    Example: python main.py --apply-mapping dia_li THPT
    """
    if len(sys.argv) < 4:
        print("Usage: python main.py --apply-mapping <subject_code> <education_level> [curriculum_year]")
        print("Example: python main.py --apply-mapping dia_li THPT 2018")
        sys.exit(1)

    from pipeline import run_apply_mapping

    subject_code = sys.argv[2]
    education_level = sys.argv[3]
    curriculum_year = int(sys.argv[4]) if len(sys.argv) > 4 else None

    covers_count = asyncio.run(run_apply_mapping(subject_code, education_level, curriculum_year))

    print("\n" + "=" * 60)
    print(f"MAPPING APPLIED: {covers_count} COVERS relationships merged")
    print("=" * 60)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    Config.validate()

    if len(sys.argv) >= 2 and sys.argv[1] == "--cli":
        cli_mode()
    elif len(sys.argv) >= 2 and sys.argv[1] == "--apply-mapping":
        apply_mapping_mode()
    else:
        asyncio.run(start_consumer())
