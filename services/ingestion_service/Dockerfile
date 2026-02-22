FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    POETRY_VERSION=1.8.0 \
    POETRY_VIRTUALENVS_CREATE=false \
    PYTHONPATH=/app

WORKDIR /app

RUN pip install --no-cache-dir "poetry==$POETRY_VERSION"

COPY pyproject.toml poetry.lock ./

# Install only main dependencies, excluding dev dependencies and the package itself
RUN poetry install --only main --no-root --no-interaction --no-ansi

COPY shared_lib/ /app/shared_lib/
COPY services/ingestion_service/ /app/services/ingestion_service/

RUN useradd -m engineuser && chown -R engineuser:engineuser /app
USER engineuser

CMD ["python", "services/ingestion_service/src/main.py"]
