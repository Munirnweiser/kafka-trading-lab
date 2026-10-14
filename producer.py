
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

producer_id = os.getenv("PRODUCER_ID", "P1")
rate = float(os.getenv("RATE_SECONDS", "1"))

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

symbols = ["EURUSD", "USDJPY", "GBPUSD", "USDCAD", "AUDUSD"]


def delivery_report(error, message):
    if error:
        print(f"DELIVERY FAILED: {error}")
    else:
        print(
            f"Producer={producer_id} "
            f"Trade={message.key().decode()} "
            f"Partition={message.partition()} "
            f"Offset={message.offset()}"
        )


print(f"Producer {producer_id} started")

try:
    while True:
        symbol = random.choice(symbols)

        trade = {
            "tradeId": f"{producer_id}-{uuid.uuid4().hex}",
            "producerId": producer_id,
            "symbol": symbol,
            "side": random.choice(["BUY", "SELL"]),
            "notional": random.randint(100_000, 5_000_000),
            "status": "NEW",
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }

        producer.produce(
            "trades",
            key=symbol,
            value=json.dumps(trade),
            callback=delivery_report,
        )

        producer.poll(0)
        time.sleep(rate)

except KeyboardInterrupt:
    print("\nStopping producer")

finally:
    producer.flush(10)

