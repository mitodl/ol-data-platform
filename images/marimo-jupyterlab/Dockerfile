# syntax=docker/dockerfile:1
# minimal-notebook provides JupyterLab, jupyterhub-singleuser, and the Z2JH
# user/entrypoint setup (jovyan, NB_UID, start.sh hooks) without the conda
# data-science stack. All notebook-level packages (pandas, trino, pyiceberg,
# etc.) are declared per-notebook via PEP 723 `/// script` inline metadata.
FROM quay.io/jupyter/minimal-notebook:latest@sha256:5db1deaf4236d9f4356a210046c6c4dc370a620a4b1843567a8c70e1b212449b

# marimo-jupyter-extension must be importable by JupyterLab — it registers
# the proxy server that forwards requests to per-notebook marimo processes.
# The extension runs this marimo as the server; --sandbox then gives each
# notebook its own uv environment built from its /// script header.
RUN --mount=type=bind,source=requirements.txt,target=/tmp/requirements.txt \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Seed notebooks and the environment README. The JupyterHub singleuser
# postStart hook copies these into ~/notebooks/ with `cp -n`, so a user's
# edited copy on the persistent home volume is never overwritten. See
# ol-infrastructure applications/jupyterhub_data/deployment.py.
COPY templates/ /usr/local/share/marimo/templates/

# Build each template's sandbox environment now, so a user's first open of a
# template doesn't download and byte-compile its dependencies.
#
# uv keeps a script's environment inside the cache directory, keyed by the
# script's path, and the environment's own files hold its absolute path. So
# both paths below have to be the ones used at runtime: the templates are
# synced from ~/notebooks (where postStart copies them), with the cache at
# UV_CACHE_DIR. The deployment mounts a node-local volume at UV_CACHE_DIR and
# fills it from UV_CACHE_SEED_DIR, which is why the finished cache is moved
# aside: a volume mounted over the build-time path would hide it.
ENV UV_CACHE_DIR=/var/cache/uv \
    UV_CACHE_SEED_DIR=/opt/uv-cache-seed
COPY --chmod=0755 prewarm_uv_cache.sh /usr/local/bin/prewarm-uv-cache
USER root
RUN install -d -o "${NB_UID}" -g "${NB_GID}" "${UV_CACHE_DIR}" "${UV_CACHE_SEED_DIR}"
USER ${NB_UID}
RUN prewarm-uv-cache
