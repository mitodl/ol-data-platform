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

# Install the JupyterLab extension (must be importable by JupyterLab) and
# marimo with sandbox extras so the marimo CLI is available for direct use.
# In sandbox mode (uvx_path configured), the extension runs each notebook via
# `uvx marimo edit --headless <file>` which installs marimo independently and
# creates a per-notebook venv from the file's `/// script` PEP 723 header.
# The conda-environment packages below are therefore NOT automatically available
# inside sandbox notebooks — each notebook must declare its own dependencies.
# They are kept here so users can reference exact version pins in their headers
# and so the marimo CLI works for non-sandbox ad-hoc use.
RUN pip install --no-cache-dir \
    'marimo[sandbox]>=0.19.11' \
    marimo-jupyter-extension \
    trino \
    'pyiceberg[s3fs,glue]>=0.9'
