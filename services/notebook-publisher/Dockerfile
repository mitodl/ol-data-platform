# syntax=docker/dockerfile:1@sha256:572cebbc2b15e6f84c077742952674889cb63cd5e9b80abdffcbe587b6f19183
FROM ghcr.io/astral-sh/uv:0.13.8@sha256:5cc1805e2426484abca4ad46ea783bab281fdb225fd82d8ce6d66a81254bac85 AS uv

FROM python:3.13-slim@sha256:e48f59cdba69a3cbd9ed2cf0f348d90d45a8d350d98134cf1070bc106ee9ed90
COPY --from=uv /uv /usr/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/notebook-publisher \
    PATH=/opt/notebook-publisher/bin:$PATH

WORKDIR /src
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-dev --no-install-project
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

# A numeric non-root user, so the pod can run under the restricted Pod Security
# Standard.
USER 1000
# marimo check reads its user config from $HOME.
ENV HOME=/tmp
WORKDIR /tmp
EXPOSE 8000
CMD ["uvicorn", "--factory", "notebook_publisher.main:create_app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
