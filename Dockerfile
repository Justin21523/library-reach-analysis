FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -g 1000 app \
    && useradd -m -u 1000 -g 1000 app

COPY pyproject.toml README.md /app/
COPY src/ /app/src/
COPY config/ /app/config/
COPY data/catalogs/ /app/data/catalogs/

RUN python -m pip install --upgrade pip \
    && python -m pip install .

RUN chown -R app:app /app

USER app

EXPOSE 8000
