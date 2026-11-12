"""OCW Studio application-database ingestion via dlt.

Loads the OCW Studio app database into QA raw (RFC 12711), the way
``mitxonline_app`` loads MITx Online's. QA's copy was Airbyte-era files converted
in place to Iceberg on 2026-09-08, except ``websites_websitecontent``, which never
converted and is invisible to StarRocks; ``dim_course_run`` reads it. Production
stays on the Airbyte connection ``OCW Studio App DB → S3 Data Lake`` until the
unit's dlt cutover.

Data flow:
    OCW Studio RDS Postgres  ->  raw__ocw__studio__postgres__<table>

Scope is the 4 tables the inventory unit ``ocw/app_postgres`` marks
``modeled: true``; the production connection syncs 34. Every table is loaded
with ``write_disposition="replace"`` and no cursor. The largest,
``websites_websitecontent``, was 212,063 rows in production raw on 2026-11-10
(Athena), with 0 duplicate primary keys across the 4 tables.

The Vault mount is the only one named per environment
(``postgres-ocw-studio-applications-qa`` and ``-production``).
"""

from typing import Any

from ol_dlt.database import (
    DatabaseSourceSpec,
    DatabaseTable,
    build_database_source,
    pipeline_for,
)

OCW_STUDIO_APP_SPEC = DatabaseSourceSpec(
    name="ocw_studio_app",
    raw_table_prefix="raw__ocw__studio__postgres__",
    database="ocw_studio",
    vault_mount="postgres-ocw-studio-applications-{profile}",
    tables=(
        DatabaseTable(name="gdrive_sync_drivefile", primary_key="file_id"),
        DatabaseTable(name="websites_website", primary_key="uuid"),
        DatabaseTable(name="websites_websitecontent", primary_key="id"),
        DatabaseTable(name="websites_websitestarter", primary_key="id"),
    ),
)

ocw_studio_app_pipeline = pipeline_for(OCW_STUDIO_APP_SPEC)


def build_source(tables: list[str] | None = None) -> Any:  # noqa: ANN401
    """Instantiate the OCW Studio app source (uniform entrypoint for Dagster)."""
    return build_database_source(OCW_STUDIO_APP_SPEC, tables=tables)
