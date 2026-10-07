"""Kafka debugging tools.

    python -m fraudx.messaging tail transactions.raw [--from-beginning] [-n 10]

Uses a throwaway consumer group and never commits, so it doesn't disturb
pipeline consumers.
"""

from __future__ import annotations

import argparse
import sys
import uuid

from confluent_kafka import Consumer

from fraudx.core.config import KafkaSettings


def tail(topic: str, from_beginning: bool, count: int | None, timeout_s: float) -> int:
    settings = KafkaSettings()
    consumer = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": f"fraudx-tail-{uuid.uuid4().hex[:8]}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
        }
    )
    consumer.subscribe([topic])
    seen, idle = 0, 0.0
    try:
        while count is None or seen < count:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1.0
                if timeout_s and idle >= timeout_s:
                    break
                continue
            idle = 0.0
            if msg.error():
                print(f"error: {msg.error()}", file=sys.stderr)
                continue
            seen += 1
            headers = dict(msg.headers() or [])
            key = (msg.key() or b"").decode(errors="replace")
            print(f"[{msg.partition()}:{msg.offset()}] key={key} headers={headers}")
            print((msg.value() or b"").decode(errors="replace"))
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m fraudx.messaging",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="command", required=True)
    t = sub.add_parser("tail", help="print messages from a topic")
    t.add_argument("topic")
    t.add_argument("--from-beginning", action="store_true")
    t.add_argument("-n", "--count", type=int, help="stop after N messages")
    t.add_argument("--timeout", type=float, default=0, help="stop after N idle seconds")
    args = parser.parse_args(argv)
    return tail(args.topic, args.from_beginning, args.count, args.timeout)


if __name__ == "__main__":
    sys.exit(main())
