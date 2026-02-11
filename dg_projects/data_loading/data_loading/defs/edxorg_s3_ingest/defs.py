"""
EdX.org S3 data ingestion definitions.

Exports the dagster assets with upstream dependencies.
"""

from .dagster_assets import edxorg_s3_consolidated_tables

# Export just the dlt assets - upstream deps are defined in edxorg code location
defs = edxorg_s3_consolidated_tables

__all__ = ["defs"]
