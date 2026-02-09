"""
RabbitMQ consumer – placeholder for future integration.

Once RabbitMQ is set up in the main system, this module will
subscribe to a queue and forward incoming messages (GCS file URIs)
to the extraction pipeline.
"""

import json
import pika
from config import Config


def _on_message(channel, method, properties, body):
    """Callback invoked for every message received from the queue."""
    message = json.loads(body)
    gcs_uri = message.get("gcs_uri", "")
    print(f"[RabbitMQ] Received message: {message}")

    # Import here to avoid circular imports
    from worker import process_file
    process_file(gcs_uri)

    channel.basic_ack(delivery_tag=method.delivery_tag)


def start_consumer() -> None:
    """Connect to RabbitMQ and start consuming messages."""
    credentials = pika.PlainCredentials(Config.RABBITMQ_USER, Config.RABBITMQ_PASSWORD)
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host=Config.RABBITMQ_HOST,
            port=Config.RABBITMQ_PORT,
            credentials=credentials,
        )
    )
    channel = connection.channel()
    channel.queue_declare(queue=Config.RABBITMQ_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=Config.RABBITMQ_QUEUE, on_message_callback=_on_message)

    print(f"[RabbitMQ] Waiting for messages on '{Config.RABBITMQ_QUEUE}' ...")
    channel.start_consuming()
