import json
import os
import signal
import sys
import time

from confluent_kafka import Consumer, KafkaError

BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GROUP_ID = os.getenv("GROUP_ID", "risk-group")
CONSUMER_NAME = os.getenv("CONSUMER_NAME", "consumer-1")

running = True


def stop_handler(signum, frame):
    global running
    running = False


signal.signal(signal.SIGINT, stop_handler)
signal.signal(signal.SIGTERM, stop_handler)

consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": GROUP_ID,
    "client.id": CONSUMER_NAME,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

consumer.subscribe(["trades"])

print(
    f"Consumer started: name={CONSUMER_NAME}, "
    f"group={GROUP_ID}. Press Ctrl+C to stop."
)

try:
    while running:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            if message.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"Kafka error: {message.error()}")
            continue

        try:
            trade = json.loads(message.value().decode("utf-8"))
        except (ValueError, UnicodeDecodeError) as error:
            print(f"Invalid trade message: {error}")
            continue

        print(
            f"[{GROUP_ID}/{CONSUMER_NAME}] "
            f"trade={trade.get('tradeId')} "
            f"symbol={trade.get('symbol')} "
            f"partition={message.partition()} "
            f"offset={message.offset()} "
            f"status={trade.get('status')}"
        )

        # Commit only after this simulated processing succeeds.
        time.sleep(0.2)
        consumer.commit(message=message, asynchronous=False)

finally:
    consumer.close()
    print(f"Consumer stopped: {CONSUMER_NAME}")
