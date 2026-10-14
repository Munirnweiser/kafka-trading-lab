import subprocess
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException

from fastapi.staticfiles import StaticFiles
import asyncio
import json
import os
from pathlib import Path
import psycopg
from dotenv import load_dotenv
from contextlib import asynccontextmanager

from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "trading_lab")
POSTGRES_USER = os.getenv("POSTGRES_USER", "trading_app")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "trades"

latest_trades = []
clients = set()
consumer_task = None


async def read_trades():
    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": "dashboard-group",
        "client.id": "trading-dashboard",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([TOPIC])
    print(f"Dashboard connected to Kafka topic: {TOPIC}")

    try:
        while True:
            message = await asyncio.to_thread(consumer.poll, 1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Kafka error: {message.error()}")
                continue

            try:
                trade = json.loads(message.value().decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                continue

            trade["_partition"] = message.partition()
            trade["_offset"] = message.offset()

            latest_trades.insert(0, trade)
            del latest_trades[100:]

            disconnected = []

            for websocket in list(clients):
                try:
                    await websocket.send_json(trade)
                except Exception:
                    disconnected.append(websocket)

            for websocket in disconnected:
                clients.discard(websocket)

    finally:
        consumer.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global consumer_task
    consumer_task = asyncio.create_task(read_trades())
    yield
    consumer_task.cancel()
    try:
        await consumer_task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="Kafka Trading Monitor", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "running", "topic": TOPIC}


@app.get("/api/trades")
async def get_trades():
    return latest_trades


@app.get("/api/kafka/consumer-groups")
async def get_kafka_consumer_groups():
    command = [
        "docker", "exec", "kafka-trading-broker",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server", "localhost:9092",
        "--describe", "--all-groups",
    ]

    result = await asyncio.to_thread(
        subprocess.run,
        command,
        capture_output=True,
        text=True,
        timeout=10,
    )

    if result.returncode != 0:
        raise HTTPException(
            status_code=503,
            detail=result.stderr.strip() or "Unable to query Kafka",
        )

    partitions = []

    for line in result.stdout.splitlines():
        columns = line.split()

        # Ignore headings and lines that aren't partition records.
        if len(columns) < 6 or columns[0] == "GROUP":
            continue

        group, topic, partition, current, end, lag = columns[:6]

        if topic != TOPIC:
            continue

        partitions.append({
            "group": group,
            "topic": topic,
            "partition": partition,
            "currentOffset": current,
            "logEndOffset": end,
            "lag": lag,
        })

    return {
        "status": "running",
        "broker": BROKER,
        "topic": TOPIC,
        "partitions": partitions,
    }



def build_positions_response(rows):
    """Format PostgreSQL's grouped position rows for the dashboard."""
    positions = []
    total_trade_count = 0

    for row in rows:
        (
            symbol,
            buy_trades,
            sell_trades,
            buy_notional,
            sell_notional,
            trade_count,
        ) = row

        buy_value = round(float(buy_notional or 0), 2)
        sell_value = round(float(sell_notional or 0), 2)
        count = int(trade_count)

        positions.append({
            "symbol": symbol,
            "buyTrades": int(buy_trades),
            "sellTrades": int(sell_trades),
            "buyNotional": buy_value,
            "sellNotional": sell_value,
            "netNotional": round(buy_value - sell_value, 2),
            "tradeCount": count,
        })
        total_trade_count += count

    return {
        "scope": "all persisted BUY/SELL trades in PostgreSQL",
        "tradeCount": total_trade_count,
        "positions": positions,
    }


@app.get("/api/positions")
def get_positions():
    """Calculate positions across all persisted valid BUY/SELL trades."""
    if not POSTGRES_PASSWORD:
        raise HTTPException(
            status_code=503,
            detail="PostgreSQL password is not configured",
        )

    query = """
        SELECT
            symbol,
            COUNT(*) FILTER (WHERE side = 'BUY') AS buy_trades,
            COUNT(*) FILTER (WHERE side = 'SELL') AS sell_trades,
            COALESCE(
                SUM(notional) FILTER (WHERE side = 'BUY'), 0
            ) AS buy_notional,
            COALESCE(
                SUM(notional) FILTER (WHERE side = 'SELL'), 0
            ) AS sell_notional,
            COUNT(*) AS trade_count
        FROM trades
        WHERE side IN ('BUY', 'SELL')
        GROUP BY symbol
        ORDER BY symbol
    """

    try:
        with psycopg.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=3,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

        return build_positions_response(rows)

    except Exception as error:
        print(f"PostgreSQL positions query failed: {error}")
        raise HTTPException(
            status_code=503,
            detail="Could not retrieve positions from PostgreSQL",
        ) from error


@app.websocket("/ws")
async def trade_stream(websocket: WebSocket):
    await websocket.accept()
    clients.add(websocket)

    try:
        for trade in latest_trades:
            await websocket.send_json(trade)

        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        clients.discard(websocket)


# Keep the static mount last so API and WebSocket routes match first.
app.mount(
    "/",
    StaticFiles(directory="dashboard/static", html=True),
    name="dashboard",
)
