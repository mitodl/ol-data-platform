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
# The extension runs this marimo as the server; --sandbox then gives each
# notebook its own uv environment built from its /// script header.
#
# marimo's AI assistant runs in that server process, not in a notebook
# environment, so its model client has to be installed here. The bedrock extra
# brings boto3, which picks up the pod's IRSA credentials. The version range is
# the one marimo's own `recommended` extra declares for pydantic-ai-slim.
# No data-science packages here: each notebook declares its own via /// script.
RUN pip install --no-cache-dir \
    'marimo[sandbox]>=0.19.11' \
    'pydantic-ai-slim[bedrock]>=1.107.0,<3.0.0' \
    marimo-jupyter-extension

# Seed notebooks and the environment README. The JupyterHub singleuser
# postStart hook copies these into ~/notebooks/ with `cp -n`, so a user's
# edited copy on the persistent home volume is never overwritten. See
# ol-infrastructure applications/jupyterhub_data/deployment.py.
COPY templates/ /usr/local/share/marimo/templates/
