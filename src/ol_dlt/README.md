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
│   ├── .pyiceberg.yaml   # aws_glue catalog config
│   └── secrets.toml.template
├── ol_dlt/
│   ├── config.py         # profile -> destination/dataset/table_format factory
│   └── sources/<name>/   # @dlt.source / @dlt.resource bodies only
└── tests/                # unit + materialization tests (ephemeral DuckDB/filesystem)
```

## Profiles

There is no `dlt.yml` / dlt+ project manifest — we run open-source dlt. The
"profile" is a plain environment variable, **`DLT_PROFILE`** (default `dev`),
read in `ol_dlt/config.py`. It is the single source of truth that replaces the
old per-`loads.py` `DAGSTER_ENVIRONMENT` branching.

| `DLT_PROFILE`     | destination                                   | `table_format` hint |
| ----------------- | --------------------------------------------- | ------------------- |
| `dev` / `ci`      | local filesystem (parquet files)              | `native`            |
| `test`            | ephemeral tmp filesystem (parquet files)      | `native`            |
| `qa`              | `s3://ol-data-lake-raw-qa/<source>` + Glue    | `iceberg`           |
| `production`      | `s3://ol-data-lake-raw-production/<source>`    | `iceberg`           |

The `table_format` column is the value `config.active_table_format()` passes to
`@dlt.resource(table_format=...)`. dlt only accepts `iceberg`/`delta`/`hive`/
`native` — **never `"parquet"`** (and not `None`). `native` means the
destination's native format, i.e. plain parquet files on the filesystem.

The `data_loading` code location maps `DAGSTER_ENVIRONMENT` → `DLT_PROFILE`
before importing pipeline factories.

## Local development loop

```bash
cd src/ol_dlt
uv sync

# run a source against the local filesystem destination
DLT_PROFILE=dev uv run python -m ol_dlt.sources.oll

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

# 4. Live standalone runs (hit real upstreams; write parquet to the local fs).
#    Credential-free sources:
DLT_PROFILE=dev uv run python -m ol_dlt.sources.oll
DLT_PROFILE=dev uv run python -m ol_dlt.sources.mitpe
DLT_PROFILE=dev uv run python -m ol_dlt.sources.mit_climate
DLT_PROFILE=dev uv run python -m ol_dlt.sources.podcast_rss
#    Needs EDX_API_* creds:
DLT_PROFILE=dev uv run python -m ol_dlt.sources.mit_edx_programs
#    Needs AWS creds (reads the prod S3 landing zone):
DLT_PROFILE=dev uv run python -m ol_dlt.sources.edxorg_s3

# 5. Inspect a pipeline's state / last load after a run.
DLT_PROFILE=dev uv run dlt pipeline oll info
```

The hermetic suite (step 1) is what CI runs and what should gate merges; the
live runs (step 4) depend on the upstream services being reachable.

## Adding a new source

1. Author the `@dlt.source` in `ol_dlt/sources/<name>/__init__.py` — no env
   branching, no Dagster imports. Resolve destination/dataset/table_format via
   `ol_dlt.config`.
2. Add a `@dlt_assets` wrapper in the `data_loading` code location
   (`defs/ingestion/assets.py`) using the shared `RawDataDltTranslator`.
