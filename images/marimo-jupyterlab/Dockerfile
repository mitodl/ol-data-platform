# syntax=docker/dockerfile:1
# Base: Jupyter scipy-notebook provides JupyterLab, jupyterhub-singleuser,
# and common data science packages (pandas, numpy, scipy, matplotlib, scikit-learn).
# All quay.io/jupyter/* images are built for Z2JH single-user server use.
FROM quay.io/jupyter/scipy-notebook:latest

# Install uv/uvx so marimo-jupyter-extension can run notebooks in sandbox
# (isolated virtual environment) mode. Copied from the official uv image to
# avoid curl-pipe-sh; this is the pattern used elsewhere in the org.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY --from=ghcr.io/astral-sh/uv:latest /uvx /usr/local/bin/uvx

# Install marimo with sandbox (uv) support and the JupyterLab extension.
# Both must be in the same environment so JupyterLab can import and proxy marimo.
# trino connects to Starburst Galaxy; TRINO_TOKEN is injected at pod launch by KubeSpawner.
# pyiceberg[s3fs,glue] enables direct Iceberg table reads from the OL data lake via IRSA.
RUN pip install --no-cache-dir \
    'marimo[sandbox]>=0.19.11' \
    marimo-jupyter-extension \
    trino \
    'pyiceberg[s3fs,glue]>=0.9'
