# Building dbt models on the local lake

The local lake is StarRocks with an Iceberg catalog (`ol_data_lake_local`) in the
ol-infrastructure local-dev cluster. It needs no Vault, VPN or AWS credentials. Bring it
up by adding `data-platform` to `enabled_apps` there; see "Data lake (Gravitino +
StarRocks)" in
[local-dev/README.md](https://github.com/mitodl/ol-infrastructure/blob/main/local-dev/README.md).
This is a different thing from the DuckDB target in [LOCAL_DEVELOPMENT.md](LOCAL_DEVELOPMENT.md),
which reads production Iceberg tables.

```bash
# Once per checkout: install the `ol-dbt` CLI into .venv.
uv sync

# In each new shell: put it on PATH. Without this, write each `ol-dbt`
# below as `uv run ol-dbt`.
source .venv/bin/activate

# Once per checkout: install the dbt packages. Without them the build stops with
# "dbt expects 6 package(s) ... found only 0".
(cd src/ol_dbt && uv run dbt deps)

# Create the raw tables the models read (ol_data_lake_local.ol_warehouse_local_raw)
ol-dbt fixtures load

# Build a model and its parents
ol-dbt starrocks build --env dev --select +integrations__learn__ocw_courses
```

Models land in `ol_warehouse_local_<layer>`. A model whose raw tables have no fixture
fails on the missing source table. The committed fixtures cover what these models and
their parents read:

| Fixture | Models |
|---|---|
| `ocw__s3.yml`, `ocw__app_postgres.yml` | `integrations__learn__ocw_courses`, `_ocw_content_files` |
| `mitxonline__app_postgres.yml` | `integrations__learn__mitxonline_courses`, `_runs`, `_programs`, `_program_children` |
| `xpro__app_postgres.yml` | `integrations__learn__xpro_courses`, `_programs`, `_runs` |

The lake holds only what you built, so `--env dev` passes dbt
`--indirect-selection buildable`: a test that also reads a model outside the selection
(a `relationships` test from some other model onto one you selected) is left out, where
dbt's default would run it and fail on the missing table. `--indirect-selection eager`
restores the default. A run with no `--select` re-selects the tests that failed last time
by name, so after a failed eager run pass `--select` again or `--full-refresh`.

## Which branches a build had

A model above staging unions several ingestion units, and it builds without error when
one of them has no rows. After a build on `starrocks_local`, `ol-dbt starrocks` counts the
raw tables each union it built reads and prints the ones that came out partial:

```text
Ingestion branches in ol_warehouse_local_raw: 0 of 1 union model(s) built have rows in every branch.
  dim_instructor: 1 of 3 branches present
    mitxonline/app_postgres: partial (2 of 3 table(s)); the tables hold no rows
      raw__mitxonline__app__postgres__cms_instructorpage (no rows), raw__mitxonline__app__postgres__cms_instructorpagelink (no rows)
    ocw/app_postgres: absent (1 table(s)); no fixture committed for this unit
    xpro/app_postgres: present
```

A branch is an inventory unit (`deployment/layer`). It is present when every raw table of
the unit that the model reads has rows, and absent when none does. The report changes
nothing about the build or its exit code, and a report that fails is skipped with one line.
The counts are of the raw schema when the build ends. A union reads its parents' tables,
so one rebuilt alone (saved state, or a `--select` without a leading `+`) may hold rows
from before the last `ol-dbt fixtures load`. The report names those unions.

A model that reads one unit is not listed, and
neither is a build on `starrocks_local_b2b`, which does not read the raw schema.

## Raw tables from a local app database

A unit with `strategies.local: ingest` in `ingestion/inventory/units/` names a dlt source
that reads the app's database in the local-dev Postgres cluster and writes its tables into
the same raw schema, so a local app's own rows can stand in for a fixture. Today those are
`mitxonline/app_postgres` and `keycloak/app_postgres`. See "Local lake" in
[src/ol_dlt/README.md](../src/ol_dlt/README.md) for the port-forwards a source needs.

```bash
ol-dbt ingest local                                 # every unit that can ingest locally
ol-dbt ingest local --unit mitxonline/app_postgres  # one of them
```

It runs each unit's source (`python -m ol_dlt.sources.<name>` in `src/ol_dlt`) with
`DLT_PROFILE=dev` whatever your shell exports, so a shell left on `qa` or `production` does
not send the load to that lake. The `<NAME>_DB_*` and `PYICEBERG_CATALOG__OL_DATA_LAKE_LOCAL__*`
overrides in the ol_dlt README still apply, for a forward on another port, and each has to
name this machine. The command refuses to start when one of the catalog's two URLs does
not, and a source fails when its `<NAME>_DB_HOST` does not, whether that came from your
shell or from `src/ol_dlt/.dlt/secrets.toml`. The inventory is validated first.

`ol-dbt fixtures load` drops and recreates the tables it has a fixture for, and the
source replaces a fixture's table that has no `_dlt_` column with its own, so load
fixtures first and the source second when both cover a table. The models then build on the app's rows:

```bash
ol-dbt fixtures load
ol-dbt ingest local --unit mitxonline/app_postgres
ol-dbt starrocks build --env dev \
    --select "+integrations__learn__mitxonline_courses +integrations__learn__mitxonline_programs +integrations__learn__mitxonline_program_children"
```

MITx Online's `populate_course_data` seed (`seed-mitxonline-course-data` in Tilt) gives
the models ten courses to build from.

## Delivering a build to a local MIT Learn

A source that reads no database needs neither a fixture nor an app. The Open Learning
Library source reads a public CSV, so its raw table lands with the catalog and object
store forwards alone:

```bash
(cd src/ol_dlt && uv run python -m ol_dlt.sources.oll)
ol-dbt starrocks build --env dev --select +integrations__learn__oll_courses
```

A webhook delivery asset reads the table that build made when `DBT_MODEL_LOCAL_LAKE=1`
is set, and posts it to the MIT Learn of the same cluster. See "Delivering what a local
dbt build made" in
[dg_projects/delivery/CONTRIBUTING.md](../dg_projects/delivery/CONTRIBUTING.md).

## Raw fixtures

The lake starts with an empty raw schema. A unit with `strategies.local: fixture` in
`ingestion/inventory/units/` has no local loader, so its raw tables come from a file of the
same name under `ingestion/inventory/fixtures/`. An `ingest` unit can have one too
(`mitxonline__app_postgres.yml`), which is what its tables hold until the source is run:

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
`--table`) into the fixture file and keeps the rows and comments already there. A nested or binary
column has no fixture type and is left out, with a warning naming it. For a table a dlt
source in this repository writes, the source's schema is the authority and no capture is
needed (`ingestion/inventory/fixtures/ocw__s3.yml` follows `ol_dlt.sources.ocw_content`).
