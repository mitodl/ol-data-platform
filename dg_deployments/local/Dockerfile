FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Disable Python downloads, because we want to use the system interpreter
# across both images. If using a managed Python version, it needs to be
# copied from the build image into the final image; see `standalone.Dockerfile`
# for an example.
ENV UV_PYTHON_DOWNLOADS=0
WORKDIR /app
# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

ENV DAGSTER_HOME=/app

EXPOSE 3000
# Copy poetry project to the $WORKDIR
COPY workspace.yaml dagster.yaml pyproject.toml uv.lock /app

RUN uv sync --locked --no-install-project
