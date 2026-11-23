#!/bin/sh
# Builds the sandbox environment marimo will use for a published notebook into
# UV_CACHE_DIR. uv keys a script's environment by the script's path, and
# marimo-operator always writes the notebook to the same one, so every
# published notebook starts from this environment and uv only adds or removes
# what its header differs by.
set -eu

notebook=/home/marimo/notebooks/notebook.py
marimo_version="$(python -c 'import marimo; print(marimo.__version__)')"

mkdir -p "$(dirname "${notebook}")"
cp /usr/local/share/marimo-runtime/prewarm_notebook.py "${notebook}"

# The sync `marimo run --sandbox` does before it relaunches itself in the
# environment, then that relaunch's marimo layer.
python_path="$(
    cd "$(dirname "${notebook}")" &&
        uv sync --script "${notebook}" --compile-bytecode --output-format json |
        python -c 'import json, sys; print(json.load(sys.stdin)["sync"]["environment"]["python"]["path"])'
)"
uv run --isolated --no-project --compile-bytecode --python "${python_path}" \
    --with "marimo==${marimo_version}" -- python -c 'import marimo'
"${python_path}" -c 'import ol_notebook.warehouse'

# The operator mounts an emptyDir here at runtime.
rm -rf "$(dirname "${notebook}")"
