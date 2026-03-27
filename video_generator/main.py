"""
RabbitMQ consumer for video generation requests — async (aio_pika).

Listens on video.generation.requests queue, processes video generation,
and publishes results to pipeline.results queue.

Supports concurrent processing: set PREFETCH_COUNT > 1 and scale
replicas in docker-compose to handle multiple users simultaneously.
"""

import asyncio
import json
import logging
import sys
import time

import aio_pika

from . import config
from .pipeline import generate_video_async
from .slide_payload_extractor import load_json_document
from .utils import extract_lesson_data

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("video_generator")


_PROGRESS_DEDUP_WINDOW_SEC = float(getattr(config, "PROGRESS_DEDUP_WINDOW_SEC", 2.0))
_recent_progress_events: dict[tuple, float] = {}
_recent_progress_events_lock = asyncio.Lock()


async def _is_duplicate_progress_event(signature: tuple) -> bool:
    """Return True when same progress event was published very recently."""
    now = time.monotonic()
    async with _recent_progress_events_lock:
        # Periodic lightweight cleanup to keep memory bounded.
        if _recent_progress_events:
            expired = [
                key
                for key, ts in _recent_progress_events.items()
                if (now - ts) > (_PROGRESS_DEDUP_WINDOW_SEC * 2)
            ]
            for key in expired:
                _recent_progress_events.pop(key, None)

        last_ts = _recent_progress_events.get(signature)
        if last_ts is not None and (now - last_ts) <= _PROGRESS_DEDUP_WINDOW_SEC:
            return True

        _recent_progress_events[signature] = now
        return False


async def get_connection() -> aio_pika.abc.AbstractRobustConnection:
    """Create a robust (auto-reconnecting) async RabbitMQ connection."""
    return await aio_pika.connect_robust(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        login=config.RABBITMQ_USER,
        password=config.RABBITMQ_PASS,
        virtualhost=config.RABBITMQ_VHOST,
    )


async def publish_result(
    channel: aio_pika.abc.AbstractChannel,
    result: dict,
    correlation_id: str | None = None,
    persistent: bool = True,
) -> None:
    """Publish a result message to the results queue."""
    await channel.default_exchange.publish(
        aio_pika.Message(
            body=json.dumps(result, ensure_ascii=False).encode(),
            delivery_mode=(
                aio_pika.DeliveryMode.PERSISTENT
                if persistent
                else aio_pika.DeliveryMode.NOT_PERSISTENT
            ),
            content_type="application/json",
            correlation_id=correlation_id,
        ),
        routing_key=config.RESULT_QUEUE,
    )
    logger.info("Published result to %s (correlation_id=%s)",
                config.RESULT_QUEUE, correlation_id)


def _extract_lesson_payload(message: dict):
    """Extract lesson payload from direct JSON data or gs:// source URI."""
    direct_payload = (
        message.get("lessonData")
        or message.get("lesson_data")
        or message.get("slideDocument")
        or message.get("slide_document")
        or message.get("slideEditedDocument")
        or message.get("slideEditedData")
        or message.get("data")
        or message.get("result")
    )
    if isinstance(direct_payload, dict):
        return direct_payload

    source = (
        message.get("lessonDataGcsUri")
        or message.get("slideDataGcsUri")
        or message.get("slideEditedDocumentUrl")
        or message.get("slideDocumentUrl")
        or message.get("slide_document_url")
        or message.get("gcsUri")
        or message.get("source")
    )
    if isinstance(source, str) and source.strip():
        return load_json_document(source.strip())

    return message


def _validate_cards_present(lesson_payload: dict) -> None:
    """Validate that resolved lesson payload contains cards for rendering."""
    lesson = extract_lesson_data(lesson_payload)
    cards = lesson.get("cards") if isinstance(lesson, dict) else None
    if isinstance(cards, list) and cards:
        return

    top_keys = sorted(list(lesson_payload.keys())) if isinstance(lesson_payload, dict) else []
    raise ValueError(
        "No cards found in video request payload. "
        "Expected one of: lessonData.cards, slideEditedDocument.cards, result.slideEditedDocument.cards, "
        "or lessonDataGcsUri/slideDataGcsUri pointing to a JSON slide document. "
        f"Received keys: {top_keys}"
    )


async def _publish_progress(
    channel: aio_pika.abc.AbstractChannel,
    task_id: str,
    user_id: str,
    product_id,
    request_id: str | None,
    status: str,
    step: str,
    progress: int,
    correlation_id: str | None = None,
    detail: str | None = None,
    result: dict | None = None,
    error: str | None = None,
) -> None:
    """Publish standard pipeline progress/result message."""
    result_signature = None
    if result is not None:
        try:
            result_signature = json.dumps(result, ensure_ascii=False, sort_keys=True)
        except TypeError:
            result_signature = str(result)

    event_signature = (
        task_id,
        user_id,
        product_id,
        request_id,
        status,
        step,
        int(progress),
        detail,
        error,
        result_signature,
    )
    if await _is_duplicate_progress_event(event_signature):
        logger.warning(
            "Suppressed duplicate progress event task=%s step=%s progress=%s",
            task_id,
            step,
            progress,
        )
        return

    payload = {
        "taskId": task_id,
        "userId": user_id,
        "productId": product_id,
        "requestId": request_id,
        "status": status,
        "step": step,
        "progress": progress,
        "detail": detail,
        "result": result,
        "error": error,
    }
    persistent = status != "processing" or bool(getattr(config, "PERSIST_PROCESSING_EVENTS", False))
    await publish_result(channel, payload, correlation_id, persistent=persistent)


async def _on_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    channel: aio_pika.abc.AbstractChannel,
) -> None:
    """Handle a single video generation task from RabbitMQ."""
    correlation_id = message.correlation_id
    task_id = correlation_id or f"msg_{message.message_id or id(message)}"
    user_id = None
    product_id = None
    source_request_id = None

    async with message.process(requeue=False):
        logger.info("Received message [%s] (%d bytes)", task_id, len(message.body))
        last_processing_event: tuple[str, int, str | None] | None = None

        try:
            msg = json.loads(message.body.decode("utf-8"))

            task_id = str(msg.get("taskId") or msg.get("task_id") or task_id)
            user_id = msg.get("userId") or msg.get("user_id")
            product_id = msg.get("productId") or msg.get("product_id")
            source_request_id = msg.get("requestId") or msg.get("videoRequestId")
            product_code = msg.get("productCode") or msg.get("product_code")

            logger.info(
                "Processing video for task=%s user=%s product=%s productCode=%s",
                task_id, user_id, product_id, product_code,
            )

            async def _publish_processing(step: str, progress: int, detail: str | None = None) -> None:
                nonlocal last_processing_event
                event = (step, int(progress), detail)
                if event == last_processing_event:
                    logger.debug("Skip duplicate progress event task=%s event=%s", task_id, event)
                    return

                last_processing_event = event
                await _publish_progress(
                    channel,
                    task_id=task_id,
                    user_id=user_id,
                    product_id=product_id,
                    request_id=source_request_id,
                    status="processing",
                    step=step,
                    progress=progress,
                    correlation_id=correlation_id,
                    detail=detail,
                )

            await _publish_processing(
                step="started",
                progress=0,
                detail="Task received, starting video generation",
            )

            await _publish_processing(
                step="loading_payload",
                progress=5,
                detail="Loading lesson payload",
            )

            # Load lesson payload (may be GCS URI or inline data)
            lesson_data = await asyncio.to_thread(
                _extract_lesson_payload, msg
            )

            await _publish_processing(
                step="validating_payload",
                progress=8,
                detail="Validating lesson structure",
            )
            _validate_cards_present(lesson_data)

            async def _pipeline_progress(step: str, progress: int, detail: str | None = None) -> None:
                await _publish_processing(step=step, progress=progress, detail=detail)

            # Run video pipeline in thread to avoid blocking event loop.
            result = await generate_video_async(
                lesson_data,
                request_id=task_id,
                progress_callback=_pipeline_progress,
            )

            result_payload = {
                "type": "video",
                "requestId": source_request_id,
                "productCode": product_code,
                "videoUrl": result["video_url"],
                "video_url": result["video_url"],
                "videoGcsUri": result.get("video_gcs_uri") or result["video_url"],
                "video_gcs_uri": result.get("video_gcs_uri") or result["video_url"],
                "videoLocalUrl": result.get("video_local_url"),
                "duration": result["duration"],
                "interactions": result.get("interactions", []),
                "pausePoints": result.get("pause_points", []),
            }

            await _publish_progress(
                channel,
                task_id=task_id,
                user_id=user_id,
                product_id=product_id,
                request_id=source_request_id,
                status="completed",
                step="video_completed",
                progress=100,
                correlation_id=correlation_id,
                detail="Video generated successfully",
                result=result_payload,
            )

            logger.info("Successfully processed [%s]", task_id)

        except json.JSONDecodeError as e:
            logger.error("Invalid JSON in message [%s]: %s", task_id, e)
            await _publish_failure(channel, task_id, user_id, product_id,
                                   source_request_id, correlation_id, str(e), "invalid_json")

        except ValueError as e:
            logger.error("Validation error [%s]: %s", task_id, e)
            await _publish_failure(channel, task_id, user_id, product_id,
                                   source_request_id, correlation_id, str(e), "validation_error")

        except Exception as e:
            logger.exception("Processing failed [%s]", task_id)
            await _publish_failure(channel, task_id, user_id, product_id,
                                   source_request_id, correlation_id,
                                   f"{type(e).__name__}: {e}", "processing_error")

async def _publish_failure(
    channel: aio_pika.abc.AbstractChannel,
    task_id: str,
    user_id,
    product_id,
    source_request_id,
    correlation_id,
    error_msg: str,
    error_type: str,
) -> None:
    await _publish_progress(
        channel,
        task_id=task_id,
        user_id=user_id,
        product_id=product_id,
        request_id=source_request_id,
        status="failed",
        step="error",
        progress=0,
        correlation_id=correlation_id,
        detail=error_type,
        error=error_msg,
    )


async def start_consumer() -> None:
    """Connect to RabbitMQ and start consuming (async, auto-reconnecting)."""
    logger.info(
        "Connecting to RabbitMQ at %s:%d ...",
        config.RABBITMQ_HOST, config.RABBITMQ_PORT,
    )

    connection = await get_connection()
    channel = await connection.channel()

    # PREFETCH_COUNT controls max concurrent video jobs per container.
    # Set to 1 (default) for single concurrent, or higher + scale replicas
    # in docker-compose for true multi-user parallelism.
    await channel.set_qos(prefetch_count=config.PREFETCH_COUNT)

    await channel.declare_queue(config.REQUEST_QUEUE, durable=True)
    await channel.declare_queue(config.RESULT_QUEUE, durable=True)

    queue = await channel.get_queue(config.REQUEST_QUEUE)

    logger.info(
        "✓ Connected. Waiting for messages on '%s' (prefetch=%d) ...",
        config.REQUEST_QUEUE, config.PREFETCH_COUNT,
    )

    await queue.consume(lambda msg: _on_message(msg, channel))

    try:
        await asyncio.Future()  # run forever
    except asyncio.CancelledError:
        logger.info("Shutting down consumer.")
        await connection.close()


if __name__ == "__main__":
    asyncio.run(start_consumer())

