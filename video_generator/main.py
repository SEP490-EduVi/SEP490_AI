"""RabbitMQ consumer for video generation requests."""

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("video_generator")

_active_tasks: set[str] = set()
_active_tasks_lock = asyncio.Lock()
_recent_task_claims: dict[str, float] = {}
_recent_task_claims_lock = asyncio.Lock()
_published_event_ids: dict[str, set[str]] = {}
_published_event_ids_lock = asyncio.Lock()


async def get_connection() -> aio_pika.abc.AbstractRobustConnection:
    """Create robust RabbitMQ connection with auto-reconnect."""
    return await aio_pika.connect_robust(
        host=config.RABBITMQ_HOST,
        port=config.RABBITMQ_PORT,
        login=config.RABBITMQ_USER,
        password=config.RABBITMQ_PASS,
        virtualhost=config.RABBITMQ_VHOST,
    )


async def publish_result(
    channel: aio_pika.abc.AbstractChannel,
    payload: dict,
    correlation_id: str | None,
    persistent: bool,
) -> None:
    """Publish a JSON message to pipeline.results."""
    await channel.default_exchange.publish(
        aio_pika.Message(
            body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
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


def _extract_lesson_payload(message: dict) -> dict:
    """Extract lesson payload from inline fields or gs:// source."""
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
    """Validate resolved payload contains cards list."""
    lesson = extract_lesson_data(lesson_payload)
    cards = lesson.get("cards") if isinstance(lesson, dict) else None
    if isinstance(cards, list) and cards:
        return

    raise ValueError("No cards found in payload.")


async def _try_claim_task(task_id: str) -> bool:
    """Claim task id to avoid duplicate processing of same message."""
    if not config.TASK_IDEMPOTENCY_ENABLED:
        return True

    now = time.monotonic()
    window = max(1.0, float(config.TASK_IDEMPOTENCY_WINDOW_SEC))

    async with _recent_task_claims_lock:
        expired = [
            key
            for key, ts in _recent_task_claims.items()
            if (now - ts) > window
        ]
        for key in expired:
            _recent_task_claims.pop(key, None)

        last = _recent_task_claims.get(task_id)
        if last is not None and (now - last) <= window:
            return False

    async with _active_tasks_lock:
        if task_id in _active_tasks:
            return False
        _active_tasks.add(task_id)

    async with _recent_task_claims_lock:
        _recent_task_claims[task_id] = now

    return True


async def _release_task(task_id: str) -> None:
    """Release in-flight task lock."""
    if not config.TASK_IDEMPOTENCY_ENABLED:
        return

    async with _active_tasks_lock:
        _active_tasks.discard(task_id)


async def _publish_progress(
    channel: aio_pika.abc.AbstractChannel,
    task_id: str,
    user_id,
    product_id,
    request_id,
    status: str,
    step: str,
    progress: int,
    correlation_id: str | None,
    detail: str | None = None,
    result: dict | None = None,
    error: str | None = None,
) -> None:
    """Publish progress/result event with per-task dedup."""
    event_id = f"{task_id}:{status}:{step}:{int(progress)}"

    async with _published_event_ids_lock:
        seen = _published_event_ids.setdefault(task_id, set())
        if event_id in seen:
            logger.debug("Skip duplicate event_id=%s", event_id)
            return
        seen.add(event_id)

    payload = {
        "taskId": task_id,
        "userId": user_id,
        "productId": product_id,
        "requestId": request_id,
        "eventId": event_id,
        "status": status,
        "step": step,
        "progress": int(progress),
        "detail": detail,
        "result": result,
        "error": error,
    }
    persistent = status != "processing" or config.PERSIST_PROCESSING_EVENTS
    await publish_result(channel, payload, correlation_id, persistent=persistent)

    if status in {"completed", "failed"}:
        async with _published_event_ids_lock:
            _published_event_ids.pop(task_id, None)


async def _publish_failure(
    channel: aio_pika.abc.AbstractChannel,
    task_id: str,
    user_id,
    product_id,
    request_id,
    correlation_id: str,
    error_type: str,
    error_message: str,
) -> None:
    await _publish_progress(
        channel=channel,
        task_id=task_id,
        user_id=user_id,
        product_id=product_id,
        request_id=request_id,
        status="failed",
        step="error",
        progress=0,
        correlation_id=correlation_id,
        detail=error_type,
        error=error_message,
    )


async def _on_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    channel: aio_pika.abc.AbstractChannel,
) -> None:
    """Handle one request message from video.generation.requests."""
    correlation_id = message.correlation_id
    default_task_id = correlation_id or f"msg_{message.message_id or id(message)}"

    async with message.process(requeue=False):
        task_id = default_task_id
        user_id = None
        product_id = None
        source_request_id = None

        try:
            data = json.loads(message.body.decode("utf-8"))

            task_id = str(data.get("taskId") or data.get("task_id") or default_task_id)
            effective_correlation_id = correlation_id or task_id
            user_id = data.get("userId") or data.get("user_id")
            product_id = data.get("productId") or data.get("product_id")
            source_request_id = data.get("requestId") or data.get("videoRequestId")
            product_code = data.get("productCode") or data.get("product_code")

            if not await _try_claim_task(task_id):
                logger.warning("Skip duplicate task message task=%s", task_id)
                return

            await _publish_progress(
                channel=channel,
                task_id=task_id,
                user_id=user_id,
                product_id=product_id,
                request_id=source_request_id,
                status="processing",
                step="started",
                progress=0,
                correlation_id=effective_correlation_id,
                detail="Task received, starting video generation",
            )

            await _publish_progress(
                channel=channel,
                task_id=task_id,
                user_id=user_id,
                product_id=product_id,
                request_id=source_request_id,
                status="processing",
                step="loading_payload",
                progress=5,
                correlation_id=effective_correlation_id,
                detail="Loading lesson payload",
            )

            lesson_payload = await asyncio.to_thread(_extract_lesson_payload, data)

            await _publish_progress(
                channel=channel,
                task_id=task_id,
                user_id=user_id,
                product_id=product_id,
                request_id=source_request_id,
                status="processing",
                step="validating_payload",
                progress=8,
                correlation_id=effective_correlation_id,
                detail="Validating lesson structure",
            )
            _validate_cards_present(lesson_payload)

            async def _pipeline_progress(step: str, progress: int, detail: str | None = None):
                await _publish_progress(
                    channel=channel,
                    task_id=task_id,
                    user_id=user_id,
                    product_id=product_id,
                    request_id=source_request_id,
                    status="processing",
                    step=step,
                    progress=progress,
                    correlation_id=effective_correlation_id,
                    detail=detail,
                )

            result = await generate_video_async(
                lesson_payload,
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
                "duration": result.get("duration", 0.0),
                "interactions": result.get("interactions", []),
                "pausePoints": result.get("pause_points", []),
                "tableOfContents": result.get("table_of_contents", []),
                "table_of_contents": result.get("table_of_contents", []),
            }

            await _publish_progress(
                channel=channel,
                task_id=task_id,
                user_id=user_id,
                product_id=product_id,
                request_id=source_request_id,
                status="completed",
                step="video_completed",
                progress=100,
                correlation_id=effective_correlation_id,
                detail="Video generated successfully",
                result=result_payload,
            )

            logger.info("Task completed: %s", task_id)

        except json.JSONDecodeError as exc:
            logger.error("Invalid JSON for task=%s: %s", task_id, exc)
            await _publish_failure(
                channel,
                task_id,
                user_id,
                product_id,
                source_request_id,
                correlation_id or task_id,
                "invalid_json",
                str(exc),
            )
        except ValueError as exc:
            logger.error("Validation error for task=%s: %s", task_id, exc)
            await _publish_failure(
                channel,
                task_id,
                user_id,
                product_id,
                source_request_id,
                correlation_id or task_id,
                "validation_error",
                str(exc),
            )
        except Exception as exc:
            logger.exception("Processing failed for task=%s", task_id)
            await _publish_failure(
                channel,
                task_id,
                user_id,
                product_id,
                source_request_id,
                correlation_id or task_id,
                "processing_error",
                f"{type(exc).__name__}: {exc}",
            )
        finally:
            await _release_task(task_id)


async def start_consumer() -> None:
    """Start RabbitMQ consumer with auto-reconnect + prefetch."""
    logger.info(
        "Connecting RabbitMQ %s:%d ...",
        config.RABBITMQ_HOST,
        config.RABBITMQ_PORT,
    )

    connection = await get_connection()
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=config.PREFETCH_COUNT)

    await channel.declare_queue(config.REQUEST_QUEUE, durable=True)
    await channel.declare_queue(config.RESULT_QUEUE, durable=True)

    queue = await channel.get_queue(config.REQUEST_QUEUE)
    await queue.consume(lambda msg: _on_message(msg, channel))

    logger.info(
        "Listening queue=%s (prefetch=%d) -> publish=%s",
        config.REQUEST_QUEUE,
        config.PREFETCH_COUNT,
        config.RESULT_QUEUE,
    )

    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        logger.info("Consumer cancelled, closing connection")
        await connection.close()


if __name__ == "__main__":
    asyncio.run(start_consumer())
