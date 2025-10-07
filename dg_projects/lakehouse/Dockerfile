# Based on: https://github.com/astral-sh/uv-docker-example/blob/main/multistage.Dockerfile

# First, build the application in the `/app` directory.
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Disable Python downloads, because we want to use the system interpreter
# across both images. If using a managed Python version, it needs to be
# copied from the build image into the final image; see `standalone.Dockerfile`
# for an example.
ENV UV_PYTHON_DOWNLOADS=0

WORKDIR /repo

# Copy the entire workspace structure needed for uv
COPY packages/ol-orchestrate-lib packages/ol-orchestrate-lib
COPY dg_projects/lakehouse dg_projects/lakehouse
COPY src/ol_dbt src/ol_dbt

# Install dependencies from the lakehouse project
WORKDIR /repo/dg_projects/lakehouse
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Sync the project itself
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev


# Then, use a final image without uv
FROM python:3.13-slim-bookworm
# It is important to use the image that matches the builder, as the path to the
# Python executable must be the same.

# Copy the application from the builder
COPY --from=builder /repo/dg_projects/lakehouse /app

# Copy the dbt project from the builder
COPY --from=builder /repo/src/ol_dbt /opt/dbt

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

WORKDIR /app

# Run dbt deps to install dbt packages (requires credentials via build secrets)
RUN --mount=type=secret,id=dbt_trino_username,env=DBT_TRINO_USERNAME \
    --mount=type=secret,id=dbt_trino_password,env=DBT_TRINO_PASSWORD \
    cd /opt/dbt && \
    /app/.venv/bin/dbt deps --project-dir /opt/dbt && \
    /app/.venv/bin/dbt ls --project-dir /opt/dbt --profiles-dir /opt/dbt --target production || echo "dbt ls failed (credentials may not be available during build)"
