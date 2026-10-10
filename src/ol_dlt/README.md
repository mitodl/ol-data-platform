# ol_dlt

Standalone [dlt](https://dlthub.com/) pipelines for the MIT Open Learning data
platform. This package owns all pure-dlt extraction code (sources, resources,
and the profile-based pipeline/destination configuration). It is wrapped by the
`data_loading` Dagster code location, which imports these sources and exposes
them as assets — this package itself **never imports Dagster** (enforced by a
ruff `banned-api` rule).

This mirrors how `src/ol_dbt` is a standalone dbt project wrapped by the
`lakehouse` code location.

## Layout

```
src/ol_dlt/
├── pyproject.toml        # package "ol-dlt"; OSS dlt + pyiceberg deps; ruff banned-api: dagster*
├── .dlt/
│   ├── config.toml       # [runtime]/[extract], [iceberg_catalog], per-source destinations
│   ├── .pyiceberg.yaml   # aws_glue and local lake catalog config
│   └── secrets.toml.template
├── ol_dlt/
│   ├── config.py         # profile -> destination/dataset/table_format factory
│   ├── database.py       # reusable relational-database source (spec -> @dlt.source)
│   ├── local_lake.py     # the local lake's catalog, as a host process reaches it
│   ├── vault.py          # Vault-issued database credentials (pure hvac)
│   └── sources/<name>/   # @dlt.source / @dlt.resource bodies only
└── tests/                # unit + materialization tests (ephemeral DuckDB/filesystem)
```

## Profiles

There is no `dlt.yml` / dlt+ project manifest — we run open-source dlt. The
"profile" is a plain environment variable, **`DLT_PROFILE`** (default `dev`),
read in `ol_dlt/config.py`. It is the single source of truth that replaces the
old per-`loads.py` `DAGSTER_ENVIRONMENT` branching.

| `DLT_PROFILE`     | destination                                         | `table_format` hint |
| ----------------- | --------------------------------------------------- | ------------------- |
| `dev`             | `s3://ol-data-lake-local/raw/<source>` + local lake | `iceberg`           |
| `ci`              | local filesystem (parquet files)                    | `native`            |
| `test`            | ephemeral tmp filesystem (parquet files)            | `native`            |
| `qa`              | `s3://ol-data-lake-raw-qa/<source>` + Glue          | `iceberg`           |
| `production`      | `s3://ol-data-lake-raw-production/<source>` + Glue  | `iceberg`           |

`dev` is the local lake of `ol-infrastructure/local-dev`: see [Local lake](#local-lake).
With no cluster running, use `ci`, which needs nothing but a filesystem.

The `table_format` column is the value `config.active_table_format()` passes to
`@dlt.resource(table_format=...)`. dlt only accepts `iceberg`/`delta`/`hive`/
`native` — **never `"parquet"`** (and not `None`). `native` means the
destination's native format, i.e. plain parquet files on the filesystem.

The `data_loading` code location maps `DAGSTER_ENVIRONMENT` → `DLT_PROFILE`
before importing pipeline factories.

## Local development loop

`ol_dlt` is a **standalone uv project** (deliberately excluded from the repo's
root uv workspace), so `uv sync` here creates a scoped `src/ol_dlt/.venv` with
only the dlt dependencies — no Dagster. `data_loading` consumes it via a path
source, not via the workspace.

```bash
cd src/ol_dlt
uv sync                       # creates a scoped, dlt-only src/ol_dlt/.venv

# run a source against the local filesystem destination (no cluster needed)
DLT_PROFILE=ci uv run python -m ol_dlt.sources.oll

# inspect a pipeline's state / last load
uv run dlt pipeline oll info

# tests (unit + materialization against the ephemeral test profile)
uv run pytest
uv run pytest -m integration   # materialization only
```

## Smoke tests — verifying the non-Dagster path

These exercise `ol_dlt` entirely on its own (no Dagster installed/imported).

```bash
cd src/ol_dlt

# 1. Hermetic suite: unit + materialization against the ephemeral test profile.
#    This is the reliable, network-free smoke test (mocks all HTTP).
uv run pytest

# 2. ol_dlt has NO dagster in its dependency closure (the separation holds).
uv tree --package ol-dlt | grep -i dagster   # expect: no output

# 3. The banned-api rule keeps dagster out of the source.
uv run ruff check --select TID251 .          # expect: All checks passed!

# 4. Live standalone runs (hit real upstreams; write Iceberg tables to the
#    local lake, or parquet to the local fs with DLT_PROFILE=ci).
#    Credential-free sources:
DLT_PROFILE=dev uv run python -m ol_dlt.sources.oll
DLT_PROFILE=dev uv run python -m ol_dlt.sources.mitpe
DLT_PROFILE=dev uv run python -m ol_dlt.sources.mit_climate
DLT_PROFILE=dev uv run python -m ol_dlt.sources.podcast_rss
#    Needs EDX_API_* creds:
DLT_PROFILE=dev uv run python -m ol_dlt.sources.mit_edx_programs
#    Needs AWS creds (reads the prod S3 landing zone):
DLT_PROFILE=dev uv run python -m ol_dlt.sources.edxorg_s3
#    Reads one hour of the PostHog export by default; --max-objects raises it:
DLT_PROFILE=dev uv run python -m ol_dlt.sources.posthog_events
#    Needs the local-dev Postgres cluster port-forwarded (see Database sources):
DLT_PROFILE=dev uv run python -m ol_dlt.sources.keycloak

# 5. Inspect a pipeline's state / last load after a run.
DLT_PROFILE=dev uv run dlt pipeline oll info
```

The hermetic suite (step 1) is what CI runs and what should gate merges; the
live runs (step 4) depend on the upstream services being reachable.

## Database sources

Relational sources do **not** get hand-written resources. Declare a
`DatabaseSourceSpec` in `ol_dlt/sources/<name>/__init__.py` and
`ol_dlt.database` builds the `@dlt.source` from it — see
`ol_dlt/sources/keycloak/` for the reference instance.

```python
KEYCLOAK_SPEC = DatabaseSourceSpec(
    name="keycloak",
    raw_table_prefix="raw__keycloak__app__postgres__",
    database="keycloak",
    vault_mount="postgres-keycloak",
    tables=(DatabaseTable(name="user_entity", primary_key="id"), ...),
)
```

**Which database gets read is the profile's job, not the spec's.** `qa` reads
the QA database, `production` reads production, and `dev`/`ci`/`test` read the
local-dev CloudNativePG cluster from `ol-infrastructure/local-dev`, so no
developer needs access to a deployed environment to exercise a pipeline. `ci`
writes parquet files and needs only the database:

```bash
kubectl port-forward -n local-infra svc/local-pg-rw 5432:5432
DLT_PROFILE=ci uv run python -m ol_dlt.sources.keycloak
```

`dev` writes the tables into the local lake: see [Local lake](#local-lake).

Per-source overrides come from `<NAME>_DB_HOST` / `_PORT` / `_USERNAME` /
`_PASSWORD`. Deployed profiles **require** `<NAME>_DB_HOST` (the Dagster Pulumi
stack passes it through from the application's stack reference) and take their
username/password from Vault at connection time — never from the environment,
and never at code-location import, where a lease would go stale long before a
scheduled run fires.

Two things that need doing outside this repo before a new database source can
run in a deployed environment, both in `ol-infrastructure`:

1. Grant `<mount>/creds/readonly` in
   `src/ol_infrastructure/applications/dagster/dagster_server_policy.hcl`.
2. Add `<NAME>_DB_HOST` to the `data_loading` deployment env in the dagster
   `__main__.py`, sourced from the application stack's exported RDS host.

## Local lake

`DLT_PROFILE=dev` writes Iceberg tables into the lake that
`ol-infrastructure/local-dev` runs when `data-platform` is in `enabled_apps`: RustFS
behind Gravitino's Iceberg REST catalog, read by StarRocks. Tables land in
`ol_data_lake_local.ol_warehouse_local_raw`, the schema the dbt sources read under
`ol-dbt starrocks --env dev`, so a source run against a local app database feeds a
local dbt build with no Vault, VPN or AWS credentials.

The pipeline runs on your machine, so it needs three port-forwards, each in its own
terminal:

```bash
kubectl -n local-infra port-forward svc/local-pg-rw 5432:5432   # the app databases
kubectl -n local-infra port-forward svc/gravitino 9001:9001     # the Iceberg REST catalog
kubectl -n local-infra port-forward svc/rustfs 9000:9000        # the object store

cd src/ol_dlt
uv run python -m ol_dlt.sources.keycloak
```

`mysql -h 127.0.0.1 -P 9030 -u root` then reads the tables:

```sql
SELECT count(*) FROM ol_data_lake_local.ol_warehouse_local_raw.raw__keycloak__app__postgres__user_entity;
```

If 9000 or 9001 is taken on your machine, forward to another port and say so:

```bash
export PYICEBERG_CATALOG__OL_DATA_LAKE_LOCAL__URI=http://localhost:19001/iceberg
export PYICEBERG_CATALOG__OL_DATA_LAKE_LOCAL__S3__ENDPOINT=http://localhost:19000
```

Gravitino tells every client to use the object store's in-cluster address, which does
not resolve on your machine. `ol_dlt/local_lake.py` keeps the forwarded one, and
explains why that needs a catalog subclass.

The `data_loading` container in `docker-compose.yaml` cannot reach those forwards, so
it sets `DLT_PROFILE=ci` and keeps writing parquet files inside the container. Run the
code location on your machine (`dagster dev`) to materialize an asset into the lake.

Two things to know when a dbt build follows a load:

- StarRocks caches a table's snapshot. Measured on StarRocks 4.1.6, it kept answering
  from the previous load for 25 to 30 seconds after a new one. `REFRESH EXTERNAL TABLE
  <catalog>.<schema>.<table>` makes it current at once.
- `ol-dbt fixtures load` writes the same schema, and drops and recreates the tables it
  has a fixture for. A `replace` source loading a table that already exists keeps the
  table, replaces its rows and adds the columns it is missing, so a fixture's table
  keeps any column only the fixture had. Drop the table first for a clean one.

## Backfilling a file-drop source

`posthog_events` resumes from a cursor it keeps in dlt state, and a cold start
with no cursor and no explicit window loads the last 7 days rather than the
whole export. Reaching further back is an explicit, chunked operation:

```bash
cd src/ol_dlt
DLT_PROFILE=production uv run python -m ol_dlt.sources.posthog_events \
    --start-date 2025-01-01 --max-objects 240
```

`--start-date` overrides the cursor, so repeat the command (dropping
`--start-date` after the first chunk) until the cursor reaches the present.
`--max-objects` bounds one run; the export is ~24 objects a day and 600+ days
deep, so an unbounded first run is not a thing to attempt.

## Adding a new source

1. Author the `@dlt.source` in `ol_dlt/sources/<name>/__init__.py` — no env
   branching, no Dagster imports. Resolve destination/dataset/table_format via
   `ol_dlt.config`. For a relational source, declare a `DatabaseSourceSpec`
   instead (see **Database sources** above).
2. Add a `@dlt_assets` wrapper in the `data_loading` code location
   (`defs/ingestion/assets.py`) using the shared `RawDataDltTranslator`.
