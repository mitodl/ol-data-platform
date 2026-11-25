# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "altair",
#     "marimo",
#     "ol-notebook",
#     "pandas",
#     "polars",
#     "pyarrow",
# ]
# ///
"""The packages a published notebook usually declares, built into the image.

prewarm_uv_cache.sh syncs this header at the path the operator writes every
published notebook to, so a notebook declaring these (or a subset) starts from
an existing environment instead of downloading them.
"""

import marimo

app = marimo.App()

if __name__ == "__main__":
    app.run()
