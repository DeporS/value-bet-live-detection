# value-bet-live-detection

## Installing poetry

- ```curl -sSL https://install.python-poetry.org | python3 -```
- export PATH="$HOME/.local/bin:$PATH"
- source ~/.bashrc

Go to project folder

- poetry install

## Run main program

PYTHONPATH=$(pwd) poetry run python services/ingestion_service/src/main.py

## Run experiments

- PYTHONPATH="$(pwd):$(pwd)/services/ingestion_service/src" poetry run python experiments/test_flashscore.py
- PYTHONPATH="$(pwd):$(pwd)/services/ingestion_service/src" poetry run python experiments/test_brotli.py

## Run just the airflow containers

docker compose up postgres-airflow airflow-webserver airflow-scheduler

## Security & Permissions (Docker Socket)

This architecture uses the "Docker-out-of-Docker" pattern (Airflow spawning external containers). For Airflow to securely communicate with the host's Docker daemon, the container needs the correct group permissions.

**Secure Setup (Recommended):**
Instead of opening the Docker socket to everyone, map your host's Docker group ID to the container environment.

1. Find your host's Docker group ID:

```bash
getent group docker | cut -d: -f3
```

2. Add it to your .env file:

```
DOCKER_GID=<your_group_id>
```

#### Actual README below

# Value Bet Live Detection

A distributed, automated system for real-time football match analysis and value bet detection. The project relies on continuous data streaming (micro-batching) and calculates "momentum" indicators on the fly to identify shifting game dynamics.

## System Architecture

The system is composed of decoupled microservices communicating via a producer-consumer model:

1. **Ingestion Service (Python / asyncio / aiohttp):**
   - Asynchronously polls target servers (Flashscore) to fetch raw goals (`dc`) and detailed statistics (`df_st`) feeds.
   - Parses proprietary text formats into structured JSON objects.
   - Operates behind a rotating residential proxy gateway to ensure high availability and prevent IP bans.
2. **Message Broker (Apache Kafka):**
   - The central nervous system. Receives raw parsed events on the `raw_match_events` topic.
3. **Streaming Processor (Apache Spark / PySpark):**
   - Consumes data from Kafka using Structured Streaming.
   - Calculates live "Momentum" metrics (e.g., differentials in possession, xG, and shots over rolling 5-minute windows).
   - Outputs processed feature sets back to Kafka (`model_features`) and archives them to Parquet files for future ML model training.
4. **Orchestrator (Apache Airflow - Upcoming):**
   - Manages the lifecycle of the system by dynamically spinning up Ingestion Service containers based on the daily match schedule.

## Engineering Highlights & Optimizations

- **Bandwidth Optimization (Brotli):** HTTP requests utilize `br` compression, reducing the payload size.
- **Geographically Targeted Proxy Rotation:** Traffic is routed through a European proxy gateway. As each request receives a new IP, loop latency naturally jitters between additional **0-2 seconds**, perfectly mimicking human-like network behavior.

## Prerequisites

- [Docker](https://www.docker.com/) & Docker Compose plugin
- A commercial Proxy provider account configured for "Rotating Gateway" with European geo-targeting.

## Quick Start

### 1. Environment Configuration

Create a `.env` file in the root directory and populate it using the template below.

```env
# --- Proxy Configuration ---
PROXY_URL=http://your_login:your_password@proxy_host:proxy_port
```

### 2. Build and Run

Build the Docker images and start the entire stack in detached mode:

```
docker compose up -d --build
```

### 3. Monitoring

To monitor the ingestion process and verify proxy tunneling:

```
docker compose logs -f ingestion_service
```

## Troubleshooting & Known Behaviors

- **Empty Spark Batches:** You might occasionally see empty batches in the Spark console. This is expected behavior. Spark queries Kafka every 5-10 seconds, but proxy latency can sometimes delay the ingestion loop to 11-12 seconds.

- **Proxy Timeouts:** Occasional TimeoutError logs are normal when using rotating residential proxies. The Circuit Breaker will handle these seamlessly unless they exceed the consecutive failure threshold.
