"""RabbitMQ utilities — async connect, publish progress/results via aio-pika."""

import json
import logging
import aio_pika
from config import Config

logger = logging.getLogger(__name__)


async def get_connection() -> aio_pika.abc.AbstractRobustConnection:
    """Create a robust (auto-reconnecting) async connection to RabbitMQ."""
    return await aio_pika.connect_robust(
        host=Config.RABBITMQ_HOST,
        port=Config.RABBITMQ_PORT,
        login=Config.RABBITMQ_USER,
        password=Config.RABBITMQ_PASSWORD,
        virtualhost=Config.RABBITMQ_VIRTUAL_HOST,
    )


async def declare_queues(channel: aio_pika.abc.AbstractChannel) -> None:
    """Declare ingestion, deletion, and result queues (idempotent)."""
    await channel.declare_queue(Config.REQUEST_QUEUE, durable=True)
    await channel.declare_queue(Config.DELETION_QUEUE, durable=True)
    await channel.declare_queue(Config.RESULT_QUEUE, durable=True)


async def publish_progress(
    channel: aio_pika.abc.AbstractChannel,
    task_id: str,
    document_id: int,
    status: str,
    step: str,
    progress: int,
    detail: str | None = None,
    stats: dict | None = None,
    error: str | None = None,
) -> None:
    """Publish a progress/result message to the textbook result queue."""
    message = {
        "taskId": task_id,
        "documentId": document_id,
        "status": status,
        "step": step,
        "progress": progress,
        "detail": detail,
        "stats": stats,
        "error": error,
    }
    await channel.default_exchange.publish(
        aio_pika.Message(
            body=json.dumps(message, ensure_ascii=False).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
        ),
        routing_key=Config.RESULT_QUEUE,
    )
    logger.info(
        "Published → %s | task=%s status=%s step=%s progress=%d",
        Config.RESULT_QUEUE, task_id, status, step, progress,
    )
