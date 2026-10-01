#!/usr/bin/env python3
"""Copy the Airbyte edX catalog history into the dlt edX discovery API tables.

The mit_edx_programs dlt source replaced the Airbyte-loaded
raw__edxorg__s3__{program,program_course,mitx_course,mitx_course_run}. Staging
keeps every program and course ever listed, so the new tables need the old
history. Run this once per environment, after the dlt load has created the new
tables there.

Airbyte loaded some landing files more than once (an overwritten file was
re-read on its new modification time), so only one row per (source file,
record key) is copied. ``_dlt_load_id`` is stamped from ``_airbyte_extracted_at``
in epoch seconds, the same form dlt's own load ids take, so the dedup macro
that orders stg__edxorg__s3__program_courses by it still prefers the newest
copy of a row. ``_dlt_id`` is minted per row.

Extractions already in the target are skipped (by ``retrieved_at``; the
pre-#2721 program_course rows have none, and are skipped if the target already
holds any such row), so a second run copies nothing.

Usage::

    # Dry run (default): report what would be copied
    uv run python bin/backfill-edxorg-catalog-raw.py --env production

    uv run python bin/backfill-edxorg-catalog-raw.py --env production --no-dry-run
"""

import logging
import secrets
from typing import Annotated, Literal

import cyclopts
import duckdb
import pyarrow as pa
from pyiceberg.catalog.glue import GlueCatalog

log = logging.getLogger(__name__)

app = cyclopts.App(help=__doc__.split("\n\n")[0])

_RAW_DATABASE = "ol_warehouse_{env}_raw"
_AWS_REGION = "us-east-1"
_DLT_ID_BYTES = 10

# Old-table suffix -> the record key a source file holds one row per.
_TABLES = {
    "program": "uuid",
    "program_course": "program_uuid, course_key",
    "mitx_course": "course_key",
    "mitx_course_run": "run_key",
}
_AIRBYTE_COLUMNS = ("_ab_source_file_url", "_airbyte_extracted_at")


def _deduplicated_history(
    catalog: GlueCatalog, database: str, table: str, target_columns: list[str]
) -> pa.Table:
    source = catalog.load_table(f"{database}.raw__edxorg__s3__{table}")
    source_columns = {field.name for field in source.schema().fields}
    data_columns = [c for c in target_columns if not c.startswith("_dlt_")]
    carried = [c for c in data_columns if c in source_columns]
    # Columns the dlt table has that Airbyte never landed (e.g. the #2721
    # program fields, or retrieved_at on an Airbyte table that predates it) come
    # through as nulls; _not_yet_loaded reads retrieved_at either way.
    missing = [c for c in data_columns if c not in source_columns]
    history = source.scan(selected_fields=(*carried, *_AIRBYTE_COLUMNS)).to_arrow()
    con = duckdb.connect()
    con.register("history", history)
    return con.sql(
        f"""
        select
            {", ".join([*carried, *(f"null::varchar as {c}" for c in missing)])},
            cast(round(_airbyte_extracted_at / 1000.0, 3) as varchar) as _dlt_load_id
        from history
        qualify row_number() over (
            partition by _ab_source_file_url, {_TABLES[table]}
            -- the latest read of a re-read file, as the Airbyte-era dedup chose
            order by _airbyte_extracted_at desc
        ) = 1
        """  # noqa: S608 -- identifiers come from _TABLES and the Iceberg schemas
    ).to_arrow_table()


def _not_yet_loaded(rows: pa.Table, existing: pa.Table) -> pa.Table:
    """Drop the rows of any extraction the target already holds."""
    con = duckdb.connect()
    con.register("rows_", rows)
    con.register("existing", existing)
    return con.sql(
        """
        select * from rows_
        where
            (retrieved_at is not null and retrieved_at not in (
                select retrieved_at from existing where retrieved_at is not null
            ))
            or (retrieved_at is null and not exists (
                select 1 from existing where retrieved_at is null
            ))
        """
    ).to_arrow_table()


def _conform(rows: pa.Table, target_schema: pa.Schema) -> pa.Table:
    """Shape ``rows`` to the target's Arrow schema, minting ``_dlt_id``."""
    columns = []
    for field in target_schema:
        if field.name == "_dlt_id":
            values = pa.array(
                [secrets.token_urlsafe(_DLT_ID_BYTES) for _ in range(rows.num_rows)]
            )
        elif field.name in rows.column_names:
            values = rows[field.name]
        else:
            values = pa.nulls(rows.num_rows)
        columns.append(values.cast(field.type))
    return pa.Table.from_arrays(columns, schema=target_schema)


@app.default
def main(
    env: Annotated[Literal["qa", "production"], cyclopts.Parameter(help="Environment")],
    *,
    dry_run: Annotated[
        bool, cyclopts.Parameter(help="Report what would be copied without writing")
    ] = True,
) -> None:
    """Backfill the four dlt edX catalog tables from their Airbyte predecessors."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    database = _RAW_DATABASE.format(env=env)
    catalog = GlueCatalog(name=database, **{"region_name": _AWS_REGION})
    for table in _TABLES:
        # QA never landed some of these (its Airbyte wrote plural names that
        # were since dropped), so there may be no history to copy.
        if not catalog.table_exists(f"{database}.raw__edxorg__s3__{table}"):
            log.warning("%s: no raw__edxorg__s3__%s in %s; skipping", table, table, env)
            continue
        target = catalog.load_table(f"{database}.raw__edxorg__discovery__api__{table}")
        target_schema = target.schema().as_arrow()
        rows = _deduplicated_history(catalog, database, table, target_schema.names)
        existing = target.scan(selected_fields=("retrieved_at",)).to_arrow()
        rows = _not_yet_loaded(rows, existing)
        log.info(
            "%s: %d deduplicated rows to copy (%d extractions); target holds %d rows",
            table,
            rows.num_rows,
            len(set(rows["retrieved_at"].to_pylist())),
            existing.num_rows,
        )
        if not dry_run and rows.num_rows:
            target.append(_conform(rows, target_schema))
            log.info("%s: appended", table)


if __name__ == "__main__":
    app()
