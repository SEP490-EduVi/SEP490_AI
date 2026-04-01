"""RabbitMQ worker for interactive game quiz generation."""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
import uuid
from typing import Any

import aio_pika

from .config import Config
from .pipeline import generate_game_async
from .slide_payload_extractor import load_json_document

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("game_quiz")

_active_tasks: set[str] = set()
_active_tasks_lock = asyncio.Lock()
_recent_task_claims: dict[str, float] = {}
_recent_task_claims_lock = asyncio.Lock()
_published_event_ids: dict[str, set[str]] = {}
_published_event_ids_lock = asyncio.Lock()


def _log_result_payload(payload: dict[str, Any]) -> None:
    if not Config.LOG_RESULT_PAYLOAD:
        return

    try:
        raw = json.dumps(payload, ensure_ascii=False)
    except (TypeError, ValueError):
        logger.info("Result payload logging skipped: not JSON serializable")
        return

    limit = max(200, int(Config.LOG_RESULT_MAX_CHARS))
    if len(raw) > limit:
        raw = raw[:limit] + "..."

    logger.info("Result payload: %s", raw)


def _preview_body(body: bytes, limit: int = 300) -> str:
    if not body:
        return "<empty>"
    try:
        text = body.decode("utf-8", errors="replace")
    except Exception:
        return "<non-utf8>"
    return text[:limit] + ("..." if len(text) > limit else "")


def _is_guid(value: str | None) -> bool:
    if not value or not isinstance(value, str):
        return False
    try:
        uuid.UUID(value)
        return True
    except (ValueError, AttributeError, TypeError):
        return False


async def get_connection() -> aio_pika.abc.AbstractRobustConnection:
    """Create robust RabbitMQ connection with auto-reconnect support."""
    return await aio_pika.connect_robust(
        host=Config.RABBITMQ_HOST,
        port=Config.RABBITMQ_PORT,
        login=Config.RABBITMQ_USER,
        password=Config.RABBITMQ_PASS,
        virtualhost=Config.RABBITMQ_VHOST,
    )


async def publish_result(
    channel: aio_pika.abc.AbstractChannel,
    payload: dict[str, Any],
    correlation_id: str | None,
    persistent: bool,
) -> None:
    """Publish one JSON result event to result queue."""
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
        routing_key=Config.RESULT_QUEUE,
    )


def _extract_slide_payload(message: dict[str, Any]) -> dict[str, Any]:
    """Extract edited slide JSON from inline fields or URL source."""
    direct_payload = (
        message.get("slideEditedDocument")
        or message.get("slide_document")
        or message.get("slideDocument")
        or message.get("slideEditedData")
        or message.get("slideData")
        or message.get("data")
        or message.get("result")
    )
    if isinstance(direct_payload, dict):
        return direct_payload

    source = (
        message.get("slideEditedDocumentUrl")
        or message.get("slideDocumentUrl")
        or message.get("slideDataGcsUri")
        or message.get("gcsUri")
        or message.get("source")
    )
    if isinstance(source, str) and source.strip():
        return load_json_document(source.strip())

    raise ValueError("Missing slideEditedDocument payload or slideEditedDocumentUrl")


async def _try_claim_task(task_id: str) -> bool:
    """Claim task id to prevent duplicate processing during idempotency window."""
    if not Config.TASK_IDEMPOTENCY_ENABLED:
        return True

    now = time.monotonic()
    window = max(1.0, float(Config.TASK_IDEMPOTENCY_WINDOW_SEC))

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
    """Release in-flight task marker."""
    if not Config.TASK_IDEMPOTENCY_ENABLED:
        return

    async with _active_tasks_lock:
        _active_tasks.discard(task_id)


async def _publish_progress(
    channel: aio_pika.abc.AbstractChannel,
    task_id: str,
    user_id: Any,
    product_id: Any,
    request_id: Any,
    template_id: str | None,
    status: str,
    step: str,
    progress: int,
    correlation_id: str | None,
    detail: str | None = None,
    result: dict[str, Any] | None = None,
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
        "templateId": template_id,
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
    persistent = status != "processing" or Config.PERSIST_PROCESSING_EVENTS
    await publish_result(channel, payload, correlation_id, persistent=persistent)

    if status in {"completed", "failed"}:
        async with _published_event_ids_lock:
            _published_event_ids.pop(task_id, None)


async def _publish_failure(
    channel: aio_pika.abc.AbstractChannel,
    task_id: str,
    user_id: Any,
    product_id: Any,
    request_id: Any,
    template_id: str | None,
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
        template_id=template_id,
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
    """Handle one request message from game quiz request queue."""
    correlation_id = message.correlation_id

    async with message.process(requeue=False):
        task_id: str | None = None
        user_id = None
        product_id = None
        source_request_id = None
        template_id: str | None = None
        effective_correlation_id = correlation_id

        try:
            data = json.loads(message.body.decode("utf-8"))

            raw_task_id = data.get("taskId") or data.get("task_id")
            task_id = str(raw_task_id).strip() if raw_task_id is not None else None
            if not _is_guid(task_id):
                logger.error("Drop message: taskId is missing/invalid GUID | body=%s", _preview_body(message.body))
                return

            effective_correlation_id = correlation_id or task_id
            user_id = data.get("userId") or data.get("user_id")
            product_id = data.get("productId") or data.get("product_id")
            source_request_id = data.get("requestId") or data.get("gameQuizRequestId")
            template_id = str(data.get("templateId") or "HOVER_SELECT").strip() or "HOVER_SELECT"
            template_json = (
                data.get("templateJson")
                or data.get("template_json")
                or data.get("template_Json")
            )
            teacher_configs = data.get("teacherConfigs") or {}
            slide_data_refs = data.get("slideDataReferences") or {}
            incoming_round_count = data.get("roundCount")
            if incoming_round_count is None:
                round_count = 1
            else:
                try:
                    round_count = int(incoming_round_count)
                except (TypeError, ValueError) as exc:
                    raise ValueError("roundCount must be an integer > 0") from exc
                if round_count <= 0:
                    raise ValueError("roundCount must be > 0")

            if not await _try_claim_task(task_id):
                logger.warning("Skip duplicate task message task=%s", task_id)
                return

            await _publish_progress(
                channel=channel,
                task_id=task_id,
                user_id=user_id,
                product_id=product_id,
                request_id=source_request_id,
                template_id=template_id,
                status="processing",
                step="started",
                progress=0,
                correlation_id=effective_correlation_id,
                detail="Task received, starting game quiz generation",
            )

            await _publish_progress(
                channel=channel,
                task_id=task_id,
                user_id=user_id,
                product_id=product_id,
                request_id=source_request_id,
                template_id=template_id,
                status="processing",
                step="loading_payload",
                progress=10,
                correlation_id=effective_correlation_id,
                detail="Loading slide and template data",
            )

            slide_payload = await asyncio.to_thread(_extract_slide_payload, data)

            async def _pipeline_progress(step: str, progress: int, detail: str | None = None):
                await _publish_progress(
                    channel=channel,
                    task_id=task_id,
                    user_id=user_id,
                    product_id=product_id,
                    request_id=source_request_id,
                    template_id=template_id,
                    status="processing",
                    step=step,
                    progress=progress,
                    correlation_id=effective_correlation_id,
                    detail=detail,
                )

            result = await generate_game_async(
                slide_payload=slide_payload,
                template_id=template_id,
                template_json_raw=template_json,
                teacher_configs=teacher_configs,
                slide_data_refs=slide_data_refs,
                game_id=task_id,
                round_count=round_count,
                progress_callback=_pipeline_progress,
            )

            _log_result_payload(result)

            await _publish_progress(
                channel=channel,
                task_id=task_id,
                user_id=user_id,
                product_id=product_id,
                request_id=source_request_id,
                template_id=template_id,
                status="completed",
                step="quiz_completed",
                progress=100,
                correlation_id=effective_correlation_id,
                detail="Interactive quiz generated successfully",
                result=result,
            )

            logger.info("Task completed: %s", task_id)

        except json.JSONDecodeError as exc:
            logger.error(
                "Drop malformed JSON request: %s | body=%s",
                exc,
                _preview_body(message.body),
            )
        except ValueError as exc:
            logger.error("Validation error for task=%s: %s", task_id, exc)
            if task_id and _is_guid(task_id):
                await _publish_failure(
                    channel,
                    task_id,
                    user_id,
                    product_id,
                    source_request_id,
                    template_id,
                    effective_correlation_id or task_id,
                    "validation_error",
                    str(exc),
                )
        except Exception as exc:
            logger.exception("Processing failed for task=%s", task_id)
            if task_id and _is_guid(task_id):
                await _publish_failure(
                    channel,
                    task_id,
                    user_id,
                    product_id,
                    source_request_id,
                    template_id,
                    effective_correlation_id or task_id,
                    "processing_error",
                    f"{type(exc).__name__}: {exc}",
                )
        finally:
            if task_id:
                await _release_task(task_id)


async def start_consumer() -> None:
    """Start RabbitMQ consumer and listen forever."""
    logger.info(
        "Connecting RabbitMQ %s:%d ...",
        Config.RABBITMQ_HOST,
        Config.RABBITMQ_PORT,
    )

    connection = await get_connection()
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=Config.PREFETCH_COUNT)

    await channel.declare_queue(Config.REQUEST_QUEUE, durable=True)
    await channel.declare_queue(Config.RESULT_QUEUE, durable=True)

    queue = await channel.get_queue(Config.REQUEST_QUEUE)
    await queue.consume(lambda msg: _on_message(msg, channel))

    logger.info(
        "Listening queue=%s (prefetch=%d) -> publish=%s",
        Config.REQUEST_QUEUE,
        Config.PREFETCH_COUNT,
        Config.RESULT_QUEUE,
    )

    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        logger.info("Consumer cancelled, closing connection")
        await connection.close()


if __name__ == "__main__":
    Config.validate()
    asyncio.run(start_consumer())
