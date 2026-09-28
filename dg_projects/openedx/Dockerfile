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
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the entire workspace structure needed for uv
COPY packages/ol-orchestrate-lib packages/ol-orchestrate-lib
COPY dg_projects/openedx dg_projects/openedx

# Install dependencies from the openedx project
WORKDIR /app/dg_projects/openedx
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
#
# PINNED, and the pins are load-bearing rather than routine hygiene. The
# preemption budget ol-infrastructure sets for run workers
# (dagster_instance.yaml: OTEL_BSP_SCHEDULE_DELAY, OTEL_EXPORTER_OTLP_TIMEOUT)
# is derived from opentelemetry-sdk 1.44.0 internals, and two of them are not
# stable API:
#   - OTEL_BSP_EXPORT_TIMEOUT is inert in 1.44.0 -- BatchProcessor stores
#     _export_timeout_millis and never reads it -- so the shutdown drain runs at
#     a hardcoded 30s join. A release that wires it up changes the budget.
#   - The Python HTTP exporter reads OTEL_EXPORTER_OTLP_TIMEOUT in SECONDS
#     (DEFAULT_TIMEOUT = 10  # in seconds), contradicting the OTel spec, which
#     defines it in milliseconds. An upstream fix would silently reinterpret
#     ol-infrastructure's "2" as 2ms and fail every export.
# opentelemetry-sdk is pinned explicitly rather than left to the distro's range
# for that reason: it is the package the reasoning actually depends on.
# Re-check that reasoning when bumping these, not just the test suite.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --python .venv \
        opentelemetry-distro==0.65b0 \
        opentelemetry-sdk==1.44.0 \
        opentelemetry-exporter-otlp-proto-http==1.44.0 \
        opentelemetry-instrumentation-sqlalchemy==0.65b0 \
        opentelemetry-instrumentation-grpc==0.65b0 \
        opentelemetry-instrumentation-botocore==0.65b0 \
        opentelemetry-instrumentation-requests==0.65b0 \
        opentelemetry-instrumentation-urllib3==0.65b0

# Then, use a final image without uv
FROM python:3.14-slim-bookworm
# It is important to use the image that matches the builder, as the path to the
# Python executable must be the same.

# Copy the application from the builder
COPY --from=builder /app/dg_projects/openedx /app

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# A stable path to the auto-instrumentation agent directory, so ol-infrastructure
# can switch tracing on with PYTHONPATH without encoding this image's Python
# version or venv layout. The directory holds a sitecustomize.py, which is the
# whole of what `opentelemetry-instrument` does: it prepends this path and execs
# the command.
#
# PYTHONPATH rather than an ENTRYPOINT because run worker pods share this image
# with the code server and the two are configured separately. An ENTRYPOINT
# applies to every process the image starts, so there would be nothing left to
# vary; a variable can be set for one workload and not another.
#
# ol-infrastructure now loads the agent on run workers too and silences the ones
# it is not measuring with OTEL_SDK_DISABLED, opted in per code location -- it
# used to strip PYTHONPATH from them with a `command` of `env -u PYTHONPATH`.
# The gRPC health-check probes still prepend that, so a probe does not pay a full
# SDK startup and emit a span for its own health check.
RUN mkdir -p /opt/otel && \
    ln -s "$(python -c 'import opentelemetry.instrumentation.auto_instrumentation as m, pathlib; print(pathlib.Path(m.__file__).parent)')" \
        /opt/otel/auto_instrumentation

WORKDIR /app
