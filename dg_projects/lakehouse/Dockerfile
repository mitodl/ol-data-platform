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

# OpenTelemetry auto-instrumentation agent, activated by PYTHONPATH (see the
# /opt/otel symlink in the final stage).
#
# Installed with `uv pip install` rather than added to pyproject.toml so the
# agent can be added or removed without a resolve across all ten code
# locations. It has to come after the syncs above, which prune anything the
# lockfile does not name. Only the HTTP OTLP exporter is installed -- the SDK
# default "otlp" resolves to the gRPC exporter, which is why ol-infrastructure
# sets OTEL_TRACES_EXPORTER=otlp_proto_http.
#
# The instrumentation list is explicit rather than whatever
# opentelemetry-bootstrap would select, because bootstrap picks psycopg2
# alongside sqlalchemy and that emits two spans for every query.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --python .venv \
        opentelemetry-distro \
        opentelemetry-exporter-otlp-proto-http \
        opentelemetry-instrumentation-sqlalchemy \
        opentelemetry-instrumentation-grpc \
        opentelemetry-instrumentation-botocore \
        opentelemetry-instrumentation-requests \
        opentelemetry-instrumentation-urllib3

# Compile dbt to generate manifest.json (CRITICAL for @dbt_assets)
WORKDIR /app/src/ol_dbt
RUN /app/dg_projects/lakehouse/.venv/bin/dbt deps && \
    /app/dg_projects/lakehouse/.venv/bin/dbt compile --target production || \
    /app/dg_projects/lakehouse/.venv/bin/dbt compile --target dev

# Generate a second manifest for the b2b_analytics models, which are only
# `+enabled` (see dbt_project.yml) when target.type == 'starrocks' and are
# therefore absent from the manifest above. `parse` (not `compile`) is used
# because it never opens a database connection, so the dummy credentials
# below only need to satisfy profiles.yml's env_var() calls, not authenticate
# against a real StarRocks cluster -- see lakehouse/assets/lakehouse/dbt_starrocks.py
# for where real Vault-issued credentials are injected at run time.
# DBT_DATA_LAKE_ENV is stated here rather than left to dbt_project.yml's
# fallback, but the value is arbitrary: one image serves every environment, so
# the baked manifest cannot name the right catalog for all of them. dbt records
# the env vars used at parse time and forces a full re-parse when they differ,
# so the run-time value injected by dbt.py / the starrocks CLI is what the
# build actually reads.
RUN DBT_STARROCKS_HOST=localhost \
    DBT_STARROCKS_USERNAME=build \
    DBT_STARROCKS_PASSWORD=build \
    DBT_DATA_LAKE_ENV=production \
    /app/dg_projects/lakehouse/.venv/bin/dbt parse \
        --target starrocks_qa_vault --target-path target/starrocks

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

# A stable path to the auto-instrumentation agent directory, so ol-infrastructure
# can switch tracing on with PYTHONPATH without encoding this image's Python
# version or venv layout. The directory holds a sitecustomize.py, which is the
# whole of what `opentelemetry-instrument` does: it prepends this path and execs
# the command.
#
# PYTHONPATH rather than an ENTRYPOINT because run worker pods share this image
# and are deliberately NOT instrumented yet; PYTHONPATH is the one of the two a
# run launcher can override back off, which is what dagster_instance.yaml in
# ol-infrastructure does. The gRPC health-check probes run `env -u PYTHONPATH`
# for the same reason.
RUN mkdir -p /opt/otel && \
    ln -s "$(python -c 'import opentelemetry.instrumentation.auto_instrumentation as m, pathlib; print(pathlib.Path(m.__file__).parent)')" \
        /opt/otel/auto_instrumentation

WORKDIR /app

# Run dbt deps to install dbt packages (requires credentials via build secrets)
RUN --mount=type=secret,id=dbt_trino_username,env=DBT_TRINO_USERNAME \
    --mount=type=secret,id=dbt_trino_password,env=DBT_TRINO_PASSWORD \
    cd /opt/dbt && \
    /app/.venv/bin/dbt deps --project-dir /opt/dbt && \
    /app/.venv/bin/dbt ls --project-dir /opt/dbt --profiles-dir /opt/dbt --target production || echo "dbt ls failed (credentials may not be available during build)"
