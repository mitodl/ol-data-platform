# Based on: https://github.com/astral-sh/uv-docker-example/blob/main/multistage.Dockerfile

# First, build the application in the `/app` directory.
FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS builder
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Disable Python downloads, because we want to use the system interpreter
# across both images. If using a managed Python version, it needs to be
# copied from the build image into the final image; see `standalone.Dockerfile`
# for an example.
ENV UV_PYTHON_DOWNLOADS=0

# Install build dependencies required for compiling C extensions (e.g., pyroaring)
# and git for uv to install dependencies from git sources (e.g., pypika)
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the entire workspace structure needed for uv
COPY packages/ol-orchestrate-lib packages/ol-orchestrate-lib
COPY dg_projects/legacy_openedx dg_projects/legacy_openedx

# Install dependencies from the legacy_openedx project
WORKDIR /app/dg_projects/legacy_openedx
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

# Then, use a final image without uv
FROM python:3.14-slim-bookworm
# It is important to use the image that matches the builder, as the path to the
# Python executable must be the same.

# Install system dependencies including MongoDB tools for forum data export
RUN apt-get update && \
    apt-get install -y wget gnupg2 && \
    apt-get clean && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Install MongoDB tools (required for edx_course_pipeline forum export)
RUN wget -qO - https://www.mongodb.org/static/pgp/server-7.0.asc | gpg --dearmor -o /usr/share/keyrings/mongodb-server-7.0.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/mongodb-server-7.0.gpg] http://repo.mongodb.org/apt/debian bookworm/mongodb-org/7.0 main" > /etc/apt/sources.list.d/mongodb-org-7.0.list && \
    apt update && \
    apt install -y mongodb-org-tools && \
    apt clean && \
    apt autoremove && \
    rm -rf /var/lib/apt/lists/*

# Copy the application from the builder
COPY --from=builder /app/dg_projects/legacy_openedx /app

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# A stable path to the auto-instrumentation agent directory, so ol-infrastructure
# can switch tracing on with PYTHONPATH without encoding this image's Python
# version or venv layout. The directory holds a sitecustomize.py, which is the
# whole of what `opentelemetry-instrument` does: it prepends this path and execs
# the command.
#
# PYTHONPATH rather than an ENTRYPOINT because run worker pods share this image
# and are deliberately NOT instrumented yet. An ENTRYPOINT applies to every
# process the image starts, so there would be nothing left to switch off; a
# variable can be stripped for one workload and not another. dagster_instance.yaml
# in ol-infrastructure strips it from run workers by giving their container a
# `command` of `env -u PYTHONPATH`, and the gRPC health-check probes prepend the
# same thing for the same reason.
RUN mkdir -p /opt/otel && \
    ln -s "$(python -c 'import opentelemetry.instrumentation.auto_instrumentation as m, pathlib; print(pathlib.Path(m.__file__).parent)')" \
        /opt/otel/auto_instrumentation

WORKDIR /app
