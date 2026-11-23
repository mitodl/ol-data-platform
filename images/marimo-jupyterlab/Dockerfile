# syntax=docker/dockerfile:1@sha256:572cebbc2b15e6f84c077742952674889cb63cd5e9b80abdffcbe587b6f19183
# minimal-notebook provides JupyterLab, jupyterhub-singleuser, and the Z2JH
# user/entrypoint setup (jovyan, NB_UID, start.sh hooks) without the conda
# data-science stack. All notebook-level packages (pandas, trino, pyiceberg,
# etc.) are declared per-notebook via PEP 723 `/// script` inline metadata.
FROM ghcr.io/astral-sh/uv:0.13.8@sha256:5cc1805e2426484abca4ad46ea783bab281fdb225fd82d8ce6d66a81254bac85 AS uv

FROM python:3.13-slim@sha256:e48f59cdba69a3cbd9ed2cf0f348d90d45a8d350d98134cf1070bc106ee9ed90 AS ol-notebook-wheel
COPY --from=uv /uv /usr/bin/uv
COPY --from=ol-notebook . /src/ol-notebook
RUN uv build --wheel --out-dir /wheels /src/ol-notebook

FROM quay.io/jupyter/minimal-notebook:latest@sha256:b97cc9adde649c3b7b8209f4aa17996b7ec0df8b5424f22aae50ce1329d900c8

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

# gh supplies the GitHub credentials for the Git panel. A user signs in once
# with `gh auth login` in a terminal, which keeps the token under ~/.config/gh
# on the persistent home volume, and /etc/gitconfig hands it to git for
# github.com remotes. These come after the cache seed so a gh bump doesn't
# rebuild it.
# renovate: datasource=conda depName=conda-forge/gh
ARG GH_VERSION=2.104.0
RUN mamba install --yes --quiet "gh=${GH_VERSION}" && \
    mamba clean --all -f -y
COPY gitconfig /etc/gitconfig

# ol_notebook (packages/ol-notebook), found by uv when a notebook names
# `ol-notebook` in its /// script header. images/marimo-runtime ships the same
# wheel, so a notebook that queries the warehouse here runs unchanged once
# published.
COPY --from=ol-notebook-wheel /wheels /opt/ol-notebook/wheels
ENV UV_FIND_LINKS=/opt/ol-notebook/wheels
