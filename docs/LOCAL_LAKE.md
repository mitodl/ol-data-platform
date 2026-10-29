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

## B2B views

The b2b materialized views (`models/b2b_analytics`, `models/b2b_learner_records`) read
thirteen dimensional tables, and the facts among them are built from Open edX grades and
tracking logs that have no local source. So they are not built from raw fixtures. A
dimensional fixture writes those tables straight into
`ol_data_lake_local.ol_warehouse_local_dimensional`, where the views' `dimensional`
source resolves under `--env dev`:

```bash
ol-dbt fixtures load-dimensional b2b_organization \
    --set sso_organization_id=<Keycloak organization id> --set contract_id=<MITx Online contract id>
ol-dbt starrocks build --env dev --target starrocks_local_b2b --select tag:starrocks

# The views refresh manually, and a build leaves them empty until they do.
for view in $(mysql -h 127.0.0.1 -P 9030 -u root -N -e \
    "SELECT CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) FROM information_schema.materialized_views
     WHERE TABLE_SCHEMA IN ('b2b_analytics', 'b2b_learner_records')"); do
  mysql -h 127.0.0.1 -P 9030 -u root -e "REFRESH MATERIALIZED VIEW ${view} WITH SYNC MODE"
done
```

The two parameters are ids the local stack assigns, and the views are only useful when
they match. ol-analytics-api authorizes a request against the Keycloak organization in
the user's token and MITx Online's manager check, and MIT Learn's dashboard reads the
organization and its contracts from MITx Online. So `sso_organization_id` is the
organization's Keycloak id (MITx Online's `OrganizationPage.sso_organization_id`, which
Keycloak picks; its admin API ignores an id in the request), and `contract_id` is the
id of a contract MITx Online holds under that organization. A run without them lists
what the file needs.

`src/ol_dbt/fixtures/dimensional/b2b_organization.yml` holds one organization, one
contract, two course runs in one program and eight learners: enough that every
per-run and per-month cohort the dashboard shows is at or above ol-analytics-api's
anonymization floor of 5, with each completion status and consent state present.
`test_dimensional_fixtures.py` runs every b2b view over it in DuckDB and fails if a view
comes back empty or entirely under the floor, so a view that starts reading another
dimensional table or column needs the fixture to follow.

- The format is the raw fixture format below, with a `parameters:` block. A row value
  that is exactly `$name` takes the value given with `--set name=...`, and a parameter's
  type has to match every column it fills.
- Each table holds only the columns the views read, and `load-dimensional` drops and
  recreates it. A dimensional model you built locally under the same name is replaced.
- Reloading drops the tables the views are built on. Build and refresh again afterwards.

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
- `ol-dbt fixtures load` drops and recreates each table, so the file and the unit's
  factory package (below) are the whole state. `--unit ocw/s3` limits it to one unit.

### Generated rows

A unit can also have a factory package, `src/ol_dbt_cli/ol_dbt_cli/fixture_factories/<unit>/`
(the contract is in that package's `__init__.py`). `ol-dbt fixtures load` runs it and
inserts the file's rows followed by the generated ones, and prints both counts per table.
Every package runs before any table is dropped, so one that fails leaves the lake as it was.

```bash
ol-dbt fixtures load --scale 100      # 100x each factory's default count; the file's rows are not scaled
ol-dbt fixtures load --seed 7         # another seed than the package's SEED, for this run
ol-dbt fixtures load --no-generate    # the files' rows only
```

To see what a package makes without a running lake, `ol-dbt fixtures generate` writes the
rows `load` would insert as JSON lines:

```bash
ol-dbt fixtures generate --unit xpro/app_postgres                     # {"raw_table": ..., "row": {...}} per line
ol-dbt fixtures generate --unit xpro/app_postgres --out /tmp/xpro     # /tmp/xpro/<raw_table>.jsonl
ol-dbt fixtures generate --unit xpro/app_postgres --table raw__xpro__app__postgres__courses_course
```

`src/ol_dbt_cli/tests/test_fixture_factories.py` runs every package in CI, with no lake.
It holds the rows to the package's `models.json` (foreign keys, generic foreign keys,
`unique`, `unique_together`, treebeard trees) and keeps generated emails on
`example.com`. It also checks that `scaffold` still writes the committed `_generated.py`,
so a hand edit there fails CI. It pins each unit's rows to a digest in
`generated_fixture_digests.json`, the sha256 of `ol-dbt fixtures generate --unit <unit>`,
which holds the hand-written rows as well as the generated ones. Any change to what `load`
inserts for the unit fails that test: an edit to the fixture file or the package, a
generation runtime change, or a mimesis or factory_boy version that draws different values.
A version bump changes every contributor's lake with no diff in the repo, and this is
what makes it visible. When the change is intended, the failure prints the new digest to
commit in the same PR.

Generated rows are fixture rows to `ol-dbt ingest local`, so "load fixtures first and
the source second" above still holds.

### Scaffolding a factory package

For a Django app, the base factories come from the app's models. `bin/django-model-dump.py`
runs with the app's own Python and writes what its models say about each table (keys,
foreign keys, `unique`, `choices`, nullability, field classes, treebeard trees) as JSON.
It imports the models and reads no rows, and it refuses a database connection, so an app
whose startup queried would fail the dump rather than reach a database. Each setting the
app's `settings.py` marks `required=True` needs a value to import, and a placeholder does
(`DATABASE_URL` has to parse as a URL, e.g. `postgres://x:x@127.0.0.1/x`):

```bash
APP=~/code/mit/repos/mitxonline
# With the app's required settings exported as placeholders:
"$APP/.venv/bin/python" bin/django-model-dump.py --app-dir "$APP" --app mitxonline \
    --settings main.settings --commit "$(git -C "$APP" rev-parse HEAD)" > /tmp/mitxonline-models.json
ol-dbt fixtures scaffold --unit mitxonline/app_postgres --models /tmp/mitxonline-models.json
```

An app with no environment of its own can get one from its lock file without touching the
checkout, e.g. `UV_PROJECT_ENVIRONMENT=/tmp/mitxpro-venv uv sync --project "$APP" --frozen --no-dev --no-install-project`.

`scaffold` writes three files in the unit's package:

- `_generated.py`: one base factory per fixture table, parents first, rewritten wholesale
  on every run. What the models cannot say (a JSON column's shape, a treebeard tree's
  `path`, a self reference, a generic foreign key's object id) is left unset with a comment.
- `models.json`: the dump cut down to the unit's fixture tables. A later `scaffold` with
  no `--models` reads it, so the package can be rescaffolded without the app until the
  fixture gains a table, which needs a new dump.
- `__init__.py`, only if there is none: the unit's `SEED` and a `generate` that adds no
  rows. The hand-written module subclasses the base factories and wires the tables together.

A nullable foreign key is NULL in a row `generate` builds without that parent. Column
names are matched without case, since Airbyte lowercases them. A column the fixture and
the models disagree on (Airbyte drops and adds columns) is printed and drawn by its
fixture type.

A unit with no Django app (an S3 or API unit such as `ocw/s3`) has no dump. With no
`--models` and no `models.json` in its package, `scaffold` drafts `_generated.py` from the
fixture's column types and the dbt tests of the staging models that `source()` each table
(read from the schema YAML under `src/ol_dbt/models/staging/`, so no warehouse or
manifest is needed; `--dbt-dir` points elsewhere):

```bash
ol-dbt fixtures scaffold --unit ocw/s3
```

A test on a staging column applies to the raw column the model passes through it,
renamed or cast and nothing more, so `page_ptr_id as wagtail_page_id` carries the tests
of `wagtail_page_id` back to `page_ptr_id`:

- `not_null` makes the column always set; a column without one is NULL in a share of rows.
  The inventory's primary key is never NULL and counts on the sequence.
- `unique` comes from the sequence, unless the column is a union of several raw columns
  (their sequences would collide) or the fixture's hand-written rows already repeat a
  value (a staging model that keeps each record's newest load tests unique what the raw
  history repeats).
- `accepted_values` cycles through the values.
- `relationships` to a model that passes the field through from another table of the
  fixture is a foreign key, drawn from its parent as a dump's would be.
- `dbt_utils.unique_combination_of_columns` and
  `dbt_expectations.expect_compound_columns_to_be_unique` become the same
  "unique together" comment a dump's `unique_together` gives.
- A string column a hand-written row holds a mapping or list in is JSON, left unset.

The tests of a derived column (`lower(title)`, a JSON extraction, a `coalesce`), a
relationship to a table the fixture leaves out, and a compound test over a derived
column are not mapped, and each is printed. No `models.json` is written: the next
`scaffold` drafts again from the tests as they are then.

### Adding a fixture

Rows are hand-written or generated, never copied from a deployed environment. Raw
application tables hold learner data, and no command here reads rows from Glue or a
database. Invent the few rows that exercise the model: one per branch of its logic is
enough. Rows that only need to exist in number belong in a factory package.

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
