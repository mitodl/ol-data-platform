# Based on: https://github.com/astral-sh/uv-docker-example/blob/main/multistage.Dockerfile

# First, build the application in the `/app` directory.
# Using Python 3.13 due to dbt-core 1.11 mashumaro constraint incompatibility with Python 3.14
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Disable Python downloads, because we want to use the system interpreter
# across both images. If using a managed Python version, it needs to be
# copied from the build image into the final image; see `standalone.Dockerfile`
# for an example.
ENV UV_PYTHON_DOWNLOADS=0

# Install build dependencies required for compiling C extensions (e.g., pyroaring)
# and git for uv to install dependencies from git sources
RUN apt-get update && \
    apt-get install --no-install-recommends -y build-essential git && \
    apt-get clean && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the entire workspace structure needed for uv
COPY packages/ol-orchestrate-lib packages/ol-orchestrate-lib
COPY dg_projects/lakehouse dg_projects/lakehouse
COPY src/ol_dbt src/ol_dbt

# Install dependencies from the lakehouse project
WORKDIR /app/dg_projects/lakehouse
RUN --mount=type=cache,target=/root/.cache/uv \
    uv venv --relocatable && \
    uv sync --frozen --no-install-project --no-dev --no-editable

# Sync the project itself
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# Compile dbt to generate manifest.json (CRITICAL for @dbt_assets)
WORKDIR /app/src/ol_dbt
RUN /app/dg_projects/lakehouse/.venv/bin/dbt deps && \
    /app/dg_projects/lakehouse/.venv/bin/dbt compile --target production || \
    /app/dg_projects/lakehouse/.venv/bin/dbt compile --target dev

# Then, use a final image without uv
# Using Python 3.13 to match builder image
FROM python:3.13-slim-bookworm
# It is important to use the image that matches the builder, as the path to the
# Python executable must be the same.

# Copy the application from the builder
COPY --from=builder /app/dg_projects/lakehouse /app
COPY --from=builder /app/src/ol_dbt /app/ol_dbt

# Copy the dbt project from the builder
COPY --from=builder /app/src/ol_dbt /opt/dbt

RUN apt-get update && \
    apt-get install --no-install-recommends -y git && \
    apt-get clean && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Run dbt deps to install dbt packages (requires credentials via build secrets)
RUN --mount=type=secret,id=dbt_trino_username,env=DBT_TRINO_USERNAME \
    --mount=type=secret,id=dbt_trino_password,env=DBT_TRINO_PASSWORD \
    cd /opt/dbt && \
    /app/.venv/bin/dbt deps --project-dir /opt/dbt && \
    /app/.venv/bin/dbt ls --project-dir /opt/dbt --profiles-dir /opt/dbt --target production || echo "dbt ls failed (credentials may not be available during build)"
