# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml .
COPY ol_superset/ ./ol_superset/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system .

ENTRYPOINT ["ol-superset"]
