"""
Textbook Ingestion Service — Async RabbitMQ Worker
===================================================
Listens on 'textbook.ingestion.requests' for ingest tasks and
'textbook.deletion.requests' for deletion tasks published by the ASP.NET backend.

Progress/results are published to 'textbook.ingestion.results'.

Local CLI testing (without RabbitMQ):
    python main.py --cli <source> <subject> <grade> [book_id]
    python main.py --cli "D:/books/dia_li_10.pdf" "Dia Li" "Lop 10"
"""

import asyncio
import io
import json
import logging
import re
import sys
import traceback

import aio_pika

from config import Config
from pipeline import run
from neo4j_loader import delete_book
from rabbitmq_utils import get_connection, declare_queues, publish_progress

if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_book_id(subject: str, grade: str) -> str:
    """Generate a safe book_id from raw strings, e.g. 'dia_li_lop_10'."""
    raw = f"{subject}_{grade}".lower()
    return re.sub(r"[^a-z0-9]+", "_", raw).strip("_")


def _build_book_id_from_codes(subject_code: str, grade_code: str) -> str:
    """
    Build book_id from RabbitMQ message fields.
    subjectCode="dia_li", gradeCode="10"  → "dia_li_lop_10"
    subjectCode="dia_li", gradeCode="lop_10" → "dia_li_lop_10" (idempotent)
    """
    grade = grade_code.strip()
    if not grade.startswith("lop_"):
        grade = f"lop_{grade}"
    return _make_book_id(subject_code, grade)


def _build_stats(structured_data: dict) -> dict:
    """Summarise ingestion counts from structured_data."""
    counts = structured_data.get("_counts", {})
    parts = structured_data.get("parts", [])
    total_chapters = sum(len(p.get("chapters", [])) for p in parts)
    total_lessons = sum(
        len(p.get("lessons", []))
        + sum(len(ch.get("lessons", [])) for ch in p.get("chapters", []))
        for p in parts
    )
    total_sections = sum(
        len(ls.get("sections", []))
        for p in parts
        for ls in p.get("lessons", [])
        + [ls for ch in p.get("chapters", []) for ls in ch.get("lessons", [])]
    )
    return {
        **counts,
        "parts": len(parts),
        "chapters": total_chapters,
        "lessons": total_lessons,
        "sections": total_sections,
    }


# ── Ingest message handler ────────────────────────────────────────────────────

async def _on_ingest_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    channel: aio_pika.abc.AbstractChannel,
) -> None:
    """Handle an incoming textbook ingestion task."""
    msg: dict = {}
    async with message.process():
        try:
            msg = json.loads(message.body)
            task_id = msg["taskId"]
            document_id = msg["documentId"]
            gcs_uri = msg["gcsUri"]
            subject_code = msg.get("subjectCode", "")
            grade_code = msg.get("gradeCode", "")
            book_id = _build_book_id_from_codes(subject_code, grade_code)

            logger.info(
                "Ingest task %s | document=%s | book=%s | gcs=%s",
                task_id, document_id, book_id, gcs_uri,
            )

            await publish_progress(
                channel, task_id, document_id,
                "processing", "started", 0,
                detail=f"Starting ingestion for book: {book_id}",
            )

            async def on_progress(step: str, progress: int, detail: str = "") -> None:
                await publish_progress(
                    channel, task_id, document_id,
                    "processing", step, progress, detail=detail,
                )

            structured_data = await run(
                source=gcs_uri,
                subject=subject_code,
                grade=grade_code,
                book_id=book_id,
                on_progress=on_progress,
            )

            stats = _build_stats(structured_data)

            await publish_progress(
                channel, task_id, document_id,
                "completed", "completed", 100,
                detail=(
                    f"Ingested {stats.get('parts', 0)} phần, "
                    f"{stats.get('chapters', 0)} chương, "
                    f"{stats.get('lessons', 0)} bài, "
                    f"{stats.get('sections', 0)} mục"
                ),
                stats=stats,
            )

            logger.info("Ingest task %s completed. Stats: %s", task_id, stats)

        except Exception as exc:
            error_detail = traceback.format_exc()
            logger.exception("Ingest task %s failed.", msg.get("taskId", "unknown"))
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


# ── Delete message handler ────────────────────────────────────────────────────

async def _on_delete_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    channel: aio_pika.abc.AbstractChannel,
) -> None:
    """Handle an incoming textbook deletion task."""
    msg: dict = {}
    async with message.process():
        try:
            msg = json.loads(message.body)
            task_id = msg["taskId"]
            document_id = msg["documentId"]
            subject_code = msg.get("subjectCode", "")
            grade_code = msg.get("gradeCode", "")
            book_id = _build_book_id_from_codes(subject_code, grade_code)

            logger.info(
                "Delete task %s | document=%s | book=%s",
                task_id, document_id, book_id,
            )

            await publish_progress(
                channel, task_id, document_id,
                "processing", "started", 0,
                detail=f"Starting deletion for book: {book_id}",
            )

            await publish_progress(
                channel, task_id, document_id,
                "processing", "deleting", 50,
                detail=f"Deleting Neo4j data for book: {book_id}",
            )

            stats = await asyncio.get_event_loop().run_in_executor(
                None, delete_book, book_id
            )

            await publish_progress(
                channel, task_id, document_id,
                "completed", "completed", 100,
                detail=(
                    f"Deleted {stats['deleted_structural']} structural nodes, "
                    f"{stats['deleted_concepts']} concept/location nodes"
                ),
                stats=stats,
            )

            logger.info("Delete task %s completed. Stats: %s", task_id, stats)

        except Exception as exc:
            error_detail = traceback.format_exc()
            logger.exception("Delete task %s failed.", msg.get("taskId", "unknown"))
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


# ── RabbitMQ Consumer ─────────────────────────────────────────────────────────

async def start_consumer() -> None:
    """Connect to RabbitMQ and start consuming both queues concurrently."""
    logger.info(
        "Connecting to RabbitMQ at %s:%s ...",
        Config.RABBITMQ_HOST, Config.RABBITMQ_PORT,
    )

    connection = await get_connection()
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=Config.PREFETCH_COUNT)
    await declare_queues(channel)

    ingest_queue = await channel.declare_queue(Config.REQUEST_QUEUE, durable=True)
    delete_queue = await channel.declare_queue(Config.DELETION_QUEUE, durable=True)

    logger.info(
        "Connected! Waiting for messages on '%s' and '%s' (prefetch=%d) ...",
        Config.REQUEST_QUEUE, Config.DELETION_QUEUE, Config.PREFETCH_COUNT,
    )

    await asyncio.gather(
        ingest_queue.consume(lambda msg: _on_ingest_message(msg, channel)),
        delete_queue.consume(lambda msg: _on_delete_message(msg, channel)),
    )

    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        logger.info("Shutting down consumer.")
        await connection.close()


# ── CLI Mode ──────────────────────────────────────────────────────────────────

def cli_mode() -> None:
    """Run the full pipeline once from CLI arguments (no RabbitMQ required)."""
    import json as _json

    if len(sys.argv) < 5:
        print("Usage: python main.py --cli <source> <subject> <grade> [book_id]")
        print('Example: python main.py --cli "D:/books/dia_li_10.pdf" "Dia Li" "Lop 10"')
        sys.exit(1)

    source = sys.argv[2]
    subject = sys.argv[3]
    grade = sys.argv[4]
    book_id = sys.argv[5] if len(sys.argv) >= 6 else _make_book_id(subject, grade)

    logger.info("=" * 60)
    logger.info("Textbook Ingestion Pipeline (CLI)")
    logger.info("  Source:  %s", source)
    logger.info("  Subject: %s", subject)
    logger.info("  Grade:   %s", grade)
    logger.info("  Book ID: %s", book_id)
    logger.info("=" * 60)

    result = asyncio.run(run(source, subject, grade, book_id))

    # Print summary
    print("\n" + "=" * 60)
    print("INGESTION COMPLETE")
    print("=" * 60)
    for part in result.get("parts", []):
        pt_id = part.get("id", "?")
        pt_name = part.get("name", "Untitled")
        print(f"\n  {pt_id}: {pt_name}")

        for ls in part.get("lessons", []):
            ls_id = ls.get("id", "?")
            ls_name = ls.get("name", "Untitled")
            sec_count = len(ls.get("sections", []))
            print(f"    {ls_id}: {ls_name} ({sec_count} sections)")

        for ch in part.get("chapters", []):
            ch_id = ch.get("id", "?")
            ch_name = ch.get("name", "Untitled")
            print(f"\n    {ch_id}: {ch_name}")
            for ls in ch.get("lessons", []):
                ls_id = ls.get("id", "?")
                ls_name = ls.get("name", "Untitled")
                sec_count = len(ls.get("sections", []))
                print(f"      {ls_id}: {ls_name} ({sec_count} sections)")

    output_path = f"output_{book_id}.json"
    with open(output_path, "w", encoding="utf-8") as f:
        _json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\nFull data saved to: {output_path}")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    Config.validate()

    if len(sys.argv) >= 2 and sys.argv[1] == "--cli":
        cli_mode()
    else:
        asyncio.run(start_consumer())

