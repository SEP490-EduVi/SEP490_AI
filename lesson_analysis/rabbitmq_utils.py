"""RabbitMQ utilities — connect, publish progress/results."""

import json
import logging
import pika
from config import Config

logger = logging.getLogger(__name__)


def get_connection() -> pika.BlockingConnection:
    """Create a blocking connection to RabbitMQ."""
    credentials = pika.PlainCredentials(Config.RABBITMQ_USER, Config.RABBITMQ_PASSWORD)
    params = pika.ConnectionParameters(
        host=Config.RABBITMQ_HOST,
        port=Config.RABBITMQ_PORT,
        virtual_host=Config.RABBITMQ_VIRTUAL_HOST,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )
    return pika.BlockingConnection(params)


def declare_queues(channel: pika.channel.Channel) -> None:
    """Declare both request and result queues (idempotent)."""
    channel.queue_declare(queue=Config.REQUEST_QUEUE, durable=True)
    channel.queue_declare(queue=Config.RESULT_QUEUE, durable=True)


def publish_progress(
    channel: pika.channel.Channel,
    task_id: str,
    user_id: str,
    product_id: int | None,
    status: str,
    step: str,
    progress: int,
    detail: str | None = None,
    result: dict | None = None,
    error: str | None = None,
) -> None:
    """Publish a progress/result message to the result queue."""
    message = {
        "taskId": task_id,
        "userId": user_id,
        "productId": product_id,
        "status": status,
        "step": step,
        "progress": progress,
        "detail": detail,
        "result": result,
        "error": error,
    }
    channel.basic_publish(
        exchange="",
        routing_key=Config.RESULT_QUEUE,
        body=json.dumps(message, ensure_ascii=False),
        properties=pika.BasicProperties(
            delivery_mode=2,  # persistent
            content_type="application/json",
        ),
    )
    logger.info(
        "Published → %s | task=%s status=%s step=%s progress=%d",
        Config.RESULT_QUEUE, task_id, status, step, progress,
    )
