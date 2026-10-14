import subprocess
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException

from fastapi.staticfiles import StaticFiles
import asyncio
import json
import os
from contextlib import asynccontextmanager

from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

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



@app.get("/api/positions")
async def get_positions():
    positions = {}

    for trade in latest_trades:
        symbol = trade.get("symbol", "UNKNOWN")
        side = str(trade.get("side", "")).upper()

        if side not in ("BUY", "SELL"):
            continue

        try:
            notional = float(trade.get("notional", 0))
        except (TypeError, ValueError):
            continue

        if notional < 0:
            continue

        if symbol not in positions:
            positions[symbol] = {
                "symbol": symbol,
                "buyTrades": 0,
                "sellTrades": 0,
                "buyNotional": 0.0,
                "sellNotional": 0.0,
            }

        item = positions[symbol]

        if side == "BUY":
            item["buyTrades"] += 1
            item["buyNotional"] += notional
        else:
            item["sellTrades"] += 1
            item["sellNotional"] += notional

    results = []

    for item in positions.values():
        item["buyNotional"] = round(item["buyNotional"], 2)
        item["sellNotional"] = round(item["sellNotional"], 2)
        item["netNotional"] = round(
            item["buyNotional"] - item["sellNotional"], 2
        )
        item["tradeCount"] = item["buyTrades"] + item["sellTrades"]
        results.append(item)

    results.sort(key=lambda item: item["symbol"])

    return {
        "scope": "latest trades held in dashboard memory",
        "tradeCount": len(latest_trades),
        "positions": results,
    }


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
