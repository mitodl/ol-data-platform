"""Standalone dlt pipelines for the MIT Open Learning data platform.

This package contains pure-dlt sources, resources, and the profile-based
pipeline/destination configuration. It must never import Dagster — Dagster
wiring lives in the ``data_loading`` code location, which imports this package.
"""

# Imported for its side effect: it wraps dlt's Iceberg merge for every source.
from ol_dlt import iceberg_upsert_guard  # noqa: F401
