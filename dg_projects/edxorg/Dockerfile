# Based on: https://github.com/astral-sh/uv-docker-example/blob/main/multistage.Dockerfile

# First, build the application in the `/app` directory.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Disable Python downloads, because we want to use the system interpreter
# across both images. If using a managed Python version, it needs to be
# copied from the build image into the final image; see `standalone.Dockerfile`
# for an example.
ENV UV_PYTHON_DOWNLOADS=0

WORKDIR /app

# Copy the entire workspace structure needed for uv
COPY packages/ol-orchestrate-lib packages/ol-orchestrate-lib
COPY dg_projects/edxorg dg_projects/edxorg
COPY src/ol_dbt src/ol_dbt

# Install dependencies from the edxorg project
WORKDIR /app/dg_projects/edxorg
RUN --mount=type=cache,target=/root/.cache/uv \
    uv venv --relocatable && \
    uv sync --frozen --no-install-project --no-dev --no-editable

# Sync the project itself
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# Compile dbt to generate manifest.json (CRITICAL for @dbt_assets)
WORKDIR /app/src/ol_dbt
RUN /app/dg_projects/edxorg/.venv/bin/dbt deps && \
    /app/dg_projects/edxorg/.venv/bin/dbt compile --target production || \
    /app/dg_projects/edxorg/.venv/bin/dbt compile --target dev


# Then, use a final image without uv
FROM python:3.13-slim-bookworm
# It is important to use the image that matches the builder, as the path to the
# Python executable must be the same.

# Copy the application from the builder
COPY --from=builder /app/dg_projects/edxorg /app
COPY --from=builder /app/src/ol_dbt /app/ol_dbt

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app
