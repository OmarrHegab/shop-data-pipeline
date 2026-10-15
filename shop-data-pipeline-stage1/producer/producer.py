"""Generate completed orders and publish JSON events to Kafka."""

import argparse
import json
import os
import random
import time
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import Producer


PRODUCTS = [
    ("product-1", "Keyboard", 49.90),
    ("product-2", "Mouse", 24.90),
    ("product-3", "Monitor", 199.90),
    ("product-4", "Headphones", 79.90),
]


def make_order():
    product_id, product_name, price = random.choice(PRODUCTS)
    return {
        "event_id": str(uuid4()),
        "order_id": str(uuid4()),
        "customer_id": f"customer-{random.randint(1, 100)}",
        "product_id": product_id,
        "product_name": product_name,
        "quantity": random.randint(1, 3),
        "unit_price_eur": price,
        "country": random.choice(["Germany", "Egypt", "France"]),
        "event_time": datetime.now(timezone.utc).isoformat(),
        "schema_version": 1,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()
    if args.count < 1 or args.interval < 0:
        parser.error("count must be >= 1 and interval must be >= 0")

    topic = os.environ.get("KAFKA_TOPIC", "orders")
    producer = Producer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "client.id": "shop-order-generator",
        "enable.idempotence": True,
        "acks": "all",
        "delivery.timeout.ms": 30000,
    })
    errors = []

    def delivered(error, message, event):
        if error:
            errors.append(str(error))
            print(f"DELIVERY FAILED: {error}", flush=True)
        else:
            print(
                f"DELIVERED topic={message.topic()} partition={message.partition()} "
                f"offset={message.offset()} event={json.dumps(event)}",
                flush=True,
            )

    for index in range(args.count):
        event = make_order()
        producer.produce(
            topic=topic,
            key=event["order_id"].encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
            on_delivery=lambda error, message, event=event: delivered(error, message, event),
        )
        # For this small learning demo, wait for confirmation of each order.
        remaining = producer.flush(timeout=35)
        if errors or remaining:
            raise RuntimeError(f"Delivery failed: errors={errors}, pending={remaining}")
        if index < args.count - 1:
            time.sleep(args.interval)

    print(f"Finished: {args.count} orders confirmed by Kafka.")


if __name__ == "__main__":
    main()
