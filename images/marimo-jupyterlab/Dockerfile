# syntax=docker/dockerfile:1
# minimal-notebook provides JupyterLab, jupyterhub-singleuser, and the Z2JH
# user/entrypoint setup (jovyan, NB_UID, start.sh hooks) without the conda
# data-science stack. All notebook-level packages (pandas, trino, pyiceberg,
# etc.) are declared per-notebook via PEP 723 `/// script` inline metadata.
FROM quay.io/jupyter/minimal-notebook:latest

# Install uv/uvx so marimo-jupyter-extension can spawn sandbox notebook
# processes. Copied from the official uv image to avoid curl-pipe-sh.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY --from=ghcr.io/astral-sh/uv:latest /uvx /usr/local/bin/uvx

# marimo-jupyter-extension must be importable by JupyterLab — it registers
# the proxy server that forwards requests to per-notebook marimo processes.
# marimo[sandbox] installs the marimo CLI that uvx invokes per notebook.
# No data-science packages here: each notebook declares its own via /// script.
RUN pip install --no-cache-dir \
    'marimo[sandbox]>=0.19.11' \
    marimo-jupyter-extension
