"""Pooled Postgres storage implementations for Dagster.

This module provides drop-in replacements for Dagster's Postgres storage classes
that use proper connection pooling (QueuePool) instead of NullPool to avoid
exhausting database connection limits.

All three storage classes (run, event log, and schedule) use the same pooling
configuration and can be configured independently or together via dagster.yaml.
"""

from ol_orchestrate.lib.postgres.event_log import PooledPostgresEventLogStorage
from ol_orchestrate.lib.postgres.run_storage import PooledPostgresRunStorage
from ol_orchestrate.lib.postgres.schedule_storage import PooledPostgresScheduleStorage

__all__ = [
    "PooledPostgresEventLogStorage",
    "PooledPostgresRunStorage",
    "PooledPostgresScheduleStorage",
]
