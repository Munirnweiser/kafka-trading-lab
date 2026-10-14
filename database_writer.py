import json
import os
import signal
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import psycopg
from psycopg.types.json import Jsonb
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaError

load_dotenv()

BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GROUP_ID = "database-writer-group"

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DB_NAME = os.getenv("POSTGRES_DB", "trading_lab")
DB_USER = os.getenv("POSTGRES_USER", "trading_app")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

running = True


def stop_handler(signum, frame):
    global running
    running = False


signal.signal(signal.SIGINT, stop_handler)
signal.signal(signal.SIGTERM, stop_handler)


def validate_trade(event):
    if not isinstance(event, dict):
        raise ValueError("Event must be a JSON object")

    trade_id = str(event.get("tradeId") or "").strip()
    producer_id = str(event.get("producerId") or "").strip()
    symbol = str(event.get("symbol") or "").strip()
    side = str(event.get("side") or "").upper()
    status = str(event.get("status") or "").strip()

    if not all([trade_id, producer_id, symbol, status]):
        raise ValueError(
            "Missing required field: tradeId, producerId, "
            "symbol, or status"
        )

    if side not in ("BUY", "SELL"):
        raise ValueError(f"Invalid or missing side: {side!r}")

    notional = Decimal(str(event.get("notional")))

    if not notional.is_finite() or notional < 0:
        raise ValueError(f"Invalid notional: {notional}")

    if not event.get("createdAt"):
        raise ValueError("Missing createdAt timestamp")

    created_at = datetime.fromisoformat(
        str(event["createdAt"]).replace("Z", "+00:00")
    )

    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)

    return (
        trade_id,
        producer_id,
        symbol,
        side,
        notional,
        status,
        created_at,
    )


INSERT_TRADE_SQL = """
    INSERT INTO trades (
        trade_id, producer_id, symbol, side, notional,
        status, created_at, kafka_topic, kafka_partition,
        kafka_offset, payload
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
"""

INSERT_DEAD_LETTER_SQL = """
    INSERT INTO trade_dead_letters (
        kafka_topic, kafka_partition, kafka_offset,
        raw_payload, error
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (kafka_topic, kafka_partition, kafka_offset)
    DO NOTHING
"""


def main():
    if not DB_PASSWORD:
        raise RuntimeError(
            "POSTGRES_PASSWORD is missing; check .env "
            "and run this script from ~/kafka-trading-lab"
        )

    connection = None
    cursor = None
    consumer = None

    try:
        connection = psycopg.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=5,
        )
        cursor = connection.cursor()

        consumer = Consumer({
            "bootstrap.servers": BROKER,
            "group.id": GROUP_ID,
            "client.id": "database-writer-1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        consumer.subscribe(["trades"])

        print(f"Connected to PostgreSQL database {DB_NAME}")
        print(f"Kafka consumer group: {GROUP_ID}")
        print("Reading topic 'trades'. Press Ctrl+C to stop.")

        processed = 0
        inserted_count = 0
        duplicate_count = 0
        dead_letter_count = 0

        while running:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Kafka error: {message.error()}", file=sys.stderr)
                continue

            raw = message.value()

            # Parse and validate separately from database operations.
            try:
                event = json.loads(raw.decode("utf-8"))
                trade = validate_trade(event)

            except (
                UnicodeDecodeError,
                json.JSONDecodeError,
                ValueError,
                TypeError,
                KeyError,
                InvalidOperation,
            ) as error:
                reason = f"{type(error).__name__}: {error}"

                # Preserve the original bytes. Do not invent missing
                # trade fields or silently discard the record.
                try:
                    cursor.execute(
                        INSERT_DEAD_LETTER_SQL,
                        (
                            message.topic(),
                            message.partition(),
                            message.offset(),
                            raw,
                            reason,
                        ),
                    )
                    connection.commit()
                except Exception as db_error:
                    connection.rollback()
                    print(
                        f"DEAD-LETTER DATABASE WRITE FAILED at "
                        f"partition={message.partition()} "
                        f"offset={message.offset()}: {db_error}",
                        file=sys.stderr,
                    )
                    break

                # Only advance Kafka after the dead letter is durable.
                try:
                    consumer.commit(
                        message=message,
                        asynchronous=False,
                    )
                except Exception as commit_error:
                    print(
                        f"Kafka offset commit failed after dead-letter "
                        f"storage: {commit_error}",
                        file=sys.stderr,
                    )
                    break

                dead_letter_count += 1
                print(
                    f"[DEAD-LETTERED] partition={message.partition()} "
                    f"offset={message.offset()} reason={reason}"
                )
                continue

            # Persist a valid trade first.
            try:
                cursor.execute(
                    INSERT_TRADE_SQL,
                    (
                        *trade,
                        message.topic(),
                        message.partition(),
                        message.offset(),
                        Jsonb(event),
                    ),
                )
                was_inserted = cursor.rowcount == 1
                connection.commit()

            except Exception as db_error:
                connection.rollback()
                print(
                    f"DATABASE WRITE FAILED at "
                    f"partition={message.partition()} "
                    f"offset={message.offset()}: {db_error}",
                    file=sys.stderr,
                )
                break

            # Then commit its Kafka offset.
            try:
                consumer.commit(
                    message=message,
                    asynchronous=False,
                )
            except Exception as commit_error:
                print(
                    f"Kafka offset commit failed after database write: "
                    f"{commit_error}",
                    file=sys.stderr,
                )
                break

            processed += 1

            if was_inserted:
                inserted_count += 1
                outcome = "INSERTED"
            else:
                duplicate_count += 1
                outcome = "DUPLICATE SKIPPED"

            if processed <= 5 or processed % 25 == 0:
                print(
                    f"[{outcome}] trade={trade[0]} symbol={trade[2]} "
                    f"partition={message.partition()} "
                    f"offset={message.offset()} processed={processed} "
                    f"inserted={inserted_count} "
                    f"duplicates={duplicate_count} "
                    f"dead_letters={dead_letter_count}"
                )

    finally:
        if consumer is not None:
            consumer.close()
        if cursor is not None:
            cursor.close()
        if connection is not None:
            connection.close()
        print("Database writer stopped.")


if __name__ == "__main__":
    main()
