"""Standalone dlt pipelines for the MIT Open Learning data platform.

This package contains pure-dlt sources, resources, and the profile-based
pipeline/destination configuration. It must never import Dagster — Dagster
wiring lives in the ``data_loading`` code location, which imports this package.
"""

import os
from pathlib import Path

# Point dlt at this project's .dlt/ (config.toml, .pyiceberg.yaml) and pyiceberg
# at the Glue catalog config, unless the runtime (e.g. Docker) already set them.
# This has to run before anything imports pyiceberg.catalog, which reads
# PYICEBERG_HOME once, at import, and iceberg_upsert_guard below does. Set any
# later, a local DLT_PROFILE=qa|production process finds no aws_glue catalog.
_PROJECT_DIR = Path(__file__).resolve().parent.parent
os.environ.setdefault("DLT_PROJECT_DIR", str(_PROJECT_DIR))
os.environ.setdefault("PYICEBERG_HOME", str(_PROJECT_DIR / ".dlt"))

# Imported for its side effect: it wraps dlt's Iceberg merge for every source.
from ol_dlt import iceberg_upsert_guard  # noqa: E402, F401
