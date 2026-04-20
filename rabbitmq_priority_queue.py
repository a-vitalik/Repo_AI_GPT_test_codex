"""RabbitMQ priority queue utilities for signal messages.

The module provides a small reusable client for publishing and consuming
signals from a priority-enabled queue. It includes a named priority constant
for urgent flush operations: ``HIGH_PRIORITY_FLUSH``.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from typing import Any, Callable

import pika

QUEUE_NAME = "signal_queue"
MAX_PRIORITY = 10
DEFAULT_PRIORITY = 1
HIGH_PRIORITY_FLUSH = 9


@dataclass(slots=True)
class SignalMessage:
    """Serialized message payload sent via RabbitMQ."""

    signal: str
    payload: dict[str, Any] | None = None

    def to_json_bytes(self) -> bytes:
        """Convert message to UTF-8 encoded JSON bytes."""
        return json.dumps(asdict(self), ensure_ascii=False).encode("utf-8")

    @classmethod
    def from_json_bytes(cls, raw_data: bytes) -> "SignalMessage":
        """Create ``SignalMessage`` from JSON bytes."""
        payload = json.loads(raw_data.decode("utf-8"))
        if "signal" not in payload:
            raise ValueError("Signal message must contain a 'signal' field")
        return cls(signal=payload["signal"], payload=payload.get("payload"))


class RabbitMQSignalQueue:
    """Client for a RabbitMQ priority queue that carries signal messages."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5672,
        username: str = "guest",
        password: str = "guest",
        queue_name: str = QUEUE_NAME,
        max_priority: int = MAX_PRIORITY,
    ) -> None:
        self.queue_name = queue_name
        self.max_priority = max_priority

        credentials = pika.PlainCredentials(username, password)
        params = pika.ConnectionParameters(host=host, port=port, credentials=credentials)
        self.connection = pika.BlockingConnection(params)
        self.channel = self.connection.channel()

        # x-max-priority enables message priorities in a queue.
        self.channel.queue_declare(
            queue=self.queue_name,
            durable=True,
            arguments={"x-max-priority": self.max_priority},
        )

    def _validate_priority(self, priority: int) -> int:
        if not 0 <= priority <= self.max_priority:
            raise ValueError(
                f"Priority must be between 0 and {self.max_priority}. Got: {priority}"
            )
        return priority

    def publish_signal(self, message: SignalMessage, priority: int = DEFAULT_PRIORITY) -> None:
        """Publish a signal to the queue with explicit priority."""
        resolved_priority = self._validate_priority(priority)

        self.channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=message.to_json_bytes(),
            properties=pika.BasicProperties(
                delivery_mode=2,  # persistent message
                priority=resolved_priority,
                content_type="application/json",
            ),
        )

    def consume_signals(self, callback: Callable[[SignalMessage, int], None]) -> None:
        """Consume queue messages forever and invoke callback for each signal."""

        def _on_message(ch, method, properties, body):
            try:
                message = SignalMessage.from_json_bytes(body)
                callback(message, properties.priority or 0)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except (json.JSONDecodeError, ValueError, TypeError):
                # Reject malformed payloads to prevent endless redelivery loop.
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=_on_message)
        self.channel.start_consuming()

    def close(self) -> None:
        """Close RabbitMQ connection."""
        if self.connection.is_open:
            self.connection.close()

    def __enter__(self) -> "RabbitMQSignalQueue":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def _build_cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="RabbitMQ priority signal queue helper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    send_parser = subparsers.add_parser("send", help="Send a signal to the queue")
    send_parser.add_argument("signal", help="Signal name, for example HIGH_PRIORITY_FLUSH")
    send_parser.add_argument(
        "--priority",
        type=int,
        default=DEFAULT_PRIORITY,
        help=f"Message priority from 0..{MAX_PRIORITY}",
    )
    send_parser.add_argument(
        "--payload",
        default="{}",
        help="JSON object payload string, for example '{\"resource\":\"cache\"}'",
    )

    subparsers.add_parser("consume", help="Consume and print incoming signals")
    return parser


def _parse_payload(payload_raw: str) -> dict[str, Any]:
    data = json.loads(payload_raw)
    if not isinstance(data, dict):
        raise ValueError("Payload must be a JSON object")
    return data


def main() -> None:
    parser = _build_cli()
    args = parser.parse_args()

    with RabbitMQSignalQueue() as queue:
        if args.command == "send":
            payload = _parse_payload(args.payload)
            queue.publish_signal(SignalMessage(signal=args.signal, payload=payload), args.priority)
            print(f"Sent signal={args.signal} with priority={args.priority}")
        else:
            print("Listening for signals... Press Ctrl+C to stop.")

            def printer(message: SignalMessage, priority: int) -> None:
                print(f"Received signal={message.signal} priority={priority} payload={message.payload}")

            queue.consume_signals(printer)


if __name__ == "__main__":
    main()
