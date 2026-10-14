# Kafka Trading Lab

A practical learning project for event streaming, Python, PostgreSQL,
FastAPI, Linux operations, Git branching, and Jenkins CI/CD.

## Architecture

- Producers publish simulated FX trades to Kafka topic `trades`.
- Independent consumer groups demonstrate risk, confirmation, and
  reconciliation processing.
- A database writer persists valid trade records in PostgreSQL.
- A FastAPI dashboard displays recent trades, Kafka monitoring,
  and simulated positions.

## Prerequisites

- Docker with Docker Compose
- Python 3
- Git
- WSL2 Ubuntu or another Linux environment

## Local setup

1. Create the local environment file:

   `cp .env.example .env`

2. Edit `.env` and set a local database password.

3. Create and activate a Python virtual environment:

   `python3 -m venv .venv`

   `source .venv/bin/activate`

4. Install Python dependencies:

   `python -m pip install -r requirements.txt`

5. Start the services:

   `docker compose up -d kafka postgres`

6. Check container status:

   `docker ps`

## Running the lab

Start the dashboard:

`uvicorn dashboard.app:app --host 0.0.0.0 --port 8000 --reload`

Start producer P1 in another terminal:

`PRODUCER_ID=P1 python producer.py`

Optionally start producer P2 in a separate terminal:

`PRODUCER_ID=P2 RATE_SECONDS=0.7 python producer.py`

Start a standalone risk consumer:

`GROUP_ID=risk-group CONSUMER_NAME=risk-consumer-1 python consumer.py`

Start the database writer in another terminal:

`python database_writer.py`

Open the dashboard at http://localhost:8000.

Run each long-running command in its own terminal. Ensure Docker services
are running before starting the Python applications.

## Database

PostgreSQL stores valid trade events in the `trades` table. Invalid or
legacy messages can be preserved in `trade_dead_letters` for investigation.

Do not delete Docker volumes when restarting the lab: they contain
persistent database data.

## Git and CI/CD

The intended workflow is:

1. Keep `main` as the stable baseline.
2. Create a feature branch for each enhancement.
3. Commit focused changes and push the branch to GitHub.
4. Use Jenkins to validate code and run tests.
5. Merge tested work through a GitHub pull request.

## Security

- Never commit `.env`, passwords, access tokens, or private keys.
- Use `.env.example` for placeholder configuration.
- This is a local learning environment, not a production trading system.
