# Building dbt models on the local lake

The local lake is StarRocks with an Iceberg catalog (`ol_data_lake_local`) in the
ol-infrastructure local-dev cluster. It needs no Vault, VPN or AWS credentials. Bring it
up by adding `data-platform` to `enabled_apps` there; see "Data lake (Gravitino +
StarRocks)" in
[local-dev/README.md](https://github.com/mitodl/ol-infrastructure/blob/main/local-dev/README.md).
This is a different thing from the DuckDB target in [LOCAL_DEVELOPMENT.md](LOCAL_DEVELOPMENT.md),
which reads production Iceberg tables.

```bash
# Create the raw tables the models read (ol_data_lake_local.ol_warehouse_local_raw)
ol-dbt fixtures load

# Build a model and its parents
ol-dbt starrocks build --env dev --select +integrations__learn__ocw_courses
```

Models land in `ol_warehouse_local_<layer>`. A model whose raw tables have no fixture
fails on the missing source table.

## Raw fixtures

The lake starts with an empty raw schema. A unit with `strategies.local: fixture` in
`ingestion/inventory/units/` has no local loader, so its raw tables come from a file of the
same name under `ingestion/inventory/fixtures/`:

```yaml
schema_version: 1
tables:
  raw__ocw__s3__course_content:
    columns:
      course_slug: string
      data_json: string
      file_size_bytes: long
      course_retrieved_at: timestamp
    rows:
    - course_slug: 99-001-local-fixtures-fall-2025
      data_json:
        course_title: Local Fixtures
      course_retrieved_at: 2026-10-01 00:00:00
```

- Column types are `boolean`, `int`, `long`, `float`, `double`, `decimal(p,s)`, `date`,
  `timestamp` and `string`. `timestamp` also stands for a column that is `timestamptz` in
  the deployed lake: StarRocks reads both as `DATETIME`.
- A column a row leaves out is NULL. A row that sets a column the table does not have is
  an error.
- A mapping or list is stored as JSON text, so a JSON-in-a-string column can be written
  as the structure it holds.
- `ol-dbt fixtures load` drops and recreates each table, so the file is the whole state.
  `--unit ocw/s3` limits it to one unit.

### Adding a fixture

Rows are written by hand and never copied from a deployed environment. Raw application
tables hold learner data, and no command here reads rows from Glue or a database. Invent
the few rows that exercise the model: one per branch of its logic is enough.

For the columns, a maintainer with AWS credentials captures them from the landed schema
once:

```bash
ol-dbt fixtures capture --unit xpro/app_postgres
```

That writes the columns and types of the unit's `modeled: true` tables (or of each
`--table`) into the fixture file and keeps any rows already there. A nested or binary
column has no fixture type and is left out, with a warning naming it. For a table a dlt
source in this repository writes, the source's schema is the authority and no capture is
needed (`ingestion/inventory/fixtures/ocw__s3.yml` follows `ol_dlt.sources.ocw_content`).
