#!/bin/sh
# Builds the sandbox environment of every shipped template into UV_CACHE_DIR,
# then moves the cache to UV_CACHE_SEED_DIR. See the Dockerfile for why the
# paths matter.
set -eu

notebooks="${HOME}/notebooks"
marimo_version="$(python -c 'import marimo; print(marimo.__version__)')"

# The server marimo-jupyter-extension starts re-launches itself under uv with
# its language-server extra layered over this interpreter.
uv run --isolated --no-project --compile-bytecode --python "$(command -v python)" \
    --with "marimo[lsp]==${marimo_version}" -- python -c 'import marimo'

mkdir -p "${notebooks}"
cp /usr/local/share/marimo/templates/*.py "${notebooks}/"

for notebook in "${notebooks}"/*.py; do
    # The same sync marimo runs when a notebook is opened with --sandbox.
    python_path="$(
        cd "${notebooks}" &&
            uv sync --script "${notebook}" --compile-bytecode --output-format json |
            python -c 'import json, sys; print(json.load(sys.stdin)["sync"]["environment"]["python"]["path"])'
    )"
    # marimo layers itself over the notebook's environment at kernel start,
    # with `uv run --active --with`. The flags differ here because there is
    # no kernel to attach to; what carries over is the cached marimo wheel.
    uv run --isolated --no-project --compile-bytecode --python "${python_path}" \
        --with "marimo==${marimo_version}" -- python -c 'import marimo'
done

rm -rf "${notebooks}"
# The directory itself is a mount point at runtime, so move its contents.
find "${UV_CACHE_DIR}" -mindepth 1 -maxdepth 1 -exec mv -t "${UV_CACHE_SEED_DIR}" {} +
