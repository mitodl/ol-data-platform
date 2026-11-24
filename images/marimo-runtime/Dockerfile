# syntax=docker/dockerfile:1@sha256:572cebbc2b15e6f84c077742952674889cb63cd5e9b80abdffcbe587b6f19183
# Runtime for notebooks published through marimo-operator (v0.3.0). The
# operator decides the layout, in pkg/resources/pod.go:
# - It runs `marimo` from PATH and mounts an emptyDir over /opt/venv, so marimo
#   has to live outside /opt/venv (marimo's own images from 0.25.2 put it there
#   and fail with "marimo: executable file not found").
# - It sets UV=/usr/bin/uv, which marimo's sandbox runs, and its setup-venv init
#   container runs `uv venv /opt/venv` from this image.
# - It puts /usr/local/lib/python3.13/site-packages on PYTHONPATH, ahead of the
#   notebook's own environment. Anything installed there would override the
#   versions a notebook declares, so marimo gets its own venv instead and that
#   directory holds only pip.
FROM ghcr.io/astral-sh/uv:0.13.8@sha256:5cc1805e2426484abca4ad46ea783bab281fdb225fd82d8ce6d66a81254bac85 AS uv

FROM python:3.13-slim@sha256:e48f59cdba69a3cbd9ed2cf0f348d90d45a8d350d98134cf1070bc106ee9ed90 AS ol-notebook-wheel
COPY --from=uv /uv /usr/bin/uv
COPY --from=ol-notebook . /src/ol-notebook
RUN uv build --wheel --out-dir /wheels /src/ol-notebook

FROM python:3.13-slim@sha256:e48f59cdba69a3cbd9ed2cf0f348d90d45a8d350d98134cf1070bc106ee9ed90
COPY --from=uv /uv /uvx /usr/bin/
RUN --mount=type=bind,source=requirements.txt,target=/tmp/requirements.txt \
    uv venv --python /usr/local/bin/python /opt/marimo && \
    uv pip install --no-cache --python /opt/marimo/bin/python -r /tmp/requirements.txt && \
    ln -s /opt/marimo/bin/marimo /usr/local/bin/marimo

# Notebooks get ol_notebook by naming `ol-notebook` in their /// script header.
# Sandbox environments don't see marimo's venv, so the wheel is served to uv
# from an index instead; image-uv.toml says why it isn't find-links.
COPY --from=ol-notebook-wheel /wheels /opt/ol-notebook/wheels
COPY --from=ol-notebook image-uv.toml /etc/uv/uv.toml
ENV UV_CACHE_DIR=/var/cache/uv

# A numeric non-root user, so the pod can set runAsNonRoot under the
# restricted Pod Security Standard. The pre-built environment below belongs
# to this UID, so pods have to run as it.
RUN useradd --uid 1000 --user-group --create-home --home-dir /home/marimo marimo && \
    install -d -o marimo -g marimo "${UV_CACHE_DIR}"

COPY prewarm_notebook.py /usr/local/share/marimo-runtime/prewarm_notebook.py
COPY --chmod=0755 prewarm_uv_cache.sh /usr/local/bin/prewarm-uv-cache
USER 1000
WORKDIR /home/marimo
RUN prewarm-uv-cache
