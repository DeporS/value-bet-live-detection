FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VERSION=1.8.0 \
    POETRY_VIRTUALENVS_CREATE=false \
    PYTHONPATH=/app

WORKDIR /app

# Install Java - needed for Spark
RUN apt-get update && \
    mkdir -p /usr/share/man/man1 && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir "poetry==$POETRY_VERSION"

COPY pyproject.toml poetry.lock ./
RUN poetry install --only main --no-root --no-interaction --no-ansi

COPY shared_lib/ /app/shared_lib/
COPY services/streaming_processor/ /app/services/streaming_processor/

RUN useradd -m sparkuser && chown -R sparkuser:sparkuser /app
USER sparkuser

CMD ["python", "services/streaming_processor/src/main.py"]