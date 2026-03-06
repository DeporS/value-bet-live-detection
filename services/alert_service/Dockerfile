FROM python:3.11-slim

RUN useradd -m alertuser

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir confluent-kafka requests psycopg2-binary

USER alertuser

CMD ["python", "/app/services/alert_service/main.py"]