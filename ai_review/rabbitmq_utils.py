"""RabbitMQ helpers for ai_review worker."""

from __future__ import annotations

import json

import aio_pika

from config import Config


async def get_connection() -> aio_pika.abc.AbstractRobustConnection:
    return await aio_pika.connect_robust(
        host=Config.RABBITMQ_HOST,
        port=Config.RABBITMQ_PORT,
        login=Config.RABBITMQ_USER,
        password=Config.RABBITMQ_PASS,
        virtualhost=Config.RABBITMQ_VHOST,
    )


async def declare_queues(channel: aio_pika.abc.AbstractChannel) -> None:
    await channel.declare_queue(Config.REQUEST_QUEUE, durable=True)
    await channel.declare_queue(Config.RESULT_QUEUE, durable=True)


async def publish_result(channel: aio_pika.abc.AbstractChannel, payload: dict) -> None:
    await channel.default_exchange.publish(
        aio_pika.Message(
            body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            content_type="application/json",
        ),
        routing_key=Config.RESULT_QUEUE,
    )
