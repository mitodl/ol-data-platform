"""
EdX.org S3 data ingestion definitions.

Exports the dagster assets with upstream dependencies.
"""

from .dagster_assets import defs

__all__ = ["defs"]
