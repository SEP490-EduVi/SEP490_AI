"""RabbitMQ worker for file review requests."""

from __future__ import annotations

import asyncio
import json
import logging
import traceback
import uuid

import aio_pika

from config import Config
from pipeline import run_review
from rabbitmq_utils import declare_queues, get_connection, publish_result

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("ai_review")


def _normalize_key(key: str) -> str:
    return "".join(ch for ch in key.lower() if ch.isalnum())


def _field(payload: dict, *aliases: str) -> str | int | None:
    normalized_payload = {_normalize_key(k): v for k, v in payload.items()}
    for alias in aliases:
        value = normalized_payload.get(_normalize_key(alias))
        if value is not None:
            return value
    return None


def _build_result_message(
    payload: dict,
    is_valid: bool,
    rejection_reason: str | None,
    summary: str,
    fallback_task_id: str | None,
    error: str | None = None,
) -> dict:
    task_id = _field(payload, "taskId", "task_id", "TaskId", "TaskID")
    if task_id is None:
        task_id = fallback_task_id or str(uuid.uuid4())

    if is_valid:
        final_rejection_reason = None
        final_summary = (summary or "Ho so hop le, staff se xem xet de phe duyet.").strip()
    else:
        final_rejection_reason = (
            (rejection_reason or "Khong du dieu kien duyet tu dong.").strip()
        )
        final_summary = "Khong du dieu kien duyet tu dong"

    return {
        "taskId": task_id,
        "expertId": _field(payload, "expertId", "expert_id", "ExpertId"),
        "reviewKind": _field(payload, "reviewKind", "review_kind", "ReviewKind"),
        "entityCode": _field(payload, "entityCode", "entity_code", "EntityCode"),
        "status": "completed",
        "progress": 100,
        "detail": "AI review completed",
        "result": {
            "isValid": bool(is_valid),
            "rejectionReason": final_rejection_reason,
            "summary": final_summary,
        },
        "error": error,
    }


async def _on_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    channel: aio_pika.abc.AbstractChannel,
) -> None:
    request: dict = {}
    async with message.process(requeue=False):
        try:
            request = json.loads(message.body)
            decision = await run_review(request)
            result_payload = _build_result_message(
                payload=request,
                is_valid=decision.is_valid,
                rejection_reason=decision.rejection_reason,
                summary=decision.summary,
                fallback_task_id=message.correlation_id,
                error=None,
            )
            await publish_result(channel, result_payload)
            logger.info(
                "Published review result taskId=%s isValid=%s",
                result_payload.get("taskId"),
                decision.is_valid,
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("Unexpected failure while handling review request")
            fallback = _build_result_message(
                payload=request,
                is_valid=False,
                rejection_reason="He thong khong the danh gia file tai thoi diem nay.",
                summary="Khong du dieu kien duyet tu dong",
                fallback_task_id=message.correlation_id,
                error=f"{type(exc).__name__}: {exc}",
            )
            await publish_result(channel, fallback)
            logger.debug("Stacktrace: %s", traceback.format_exc())


async def start_consumer() -> None:
    Config.validate()
    connection = await get_connection()
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=Config.PREFETCH_COUNT)
    await declare_queues(channel)

    queue = await channel.declare_queue(Config.REQUEST_QUEUE, durable=True)
    logger.info(
        "ai_review listening queue=%s resultQueue=%s prefetch=%s",
        Config.REQUEST_QUEUE,
        Config.RESULT_QUEUE,
        Config.PREFETCH_COUNT,
    )

    await queue.consume(lambda msg: _on_message(msg, channel))
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(start_consumer())
