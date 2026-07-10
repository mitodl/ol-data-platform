---
name: ol-dbt-local-dev
description: >
  Stand up and drive the local DuckDB + Iceberg dbt development environment via
  `ol-dbt local` and `ol-dbt run`. Use this skill when you need to build or
  iterate on src/ol_dbt models locally against real production data (zero-copy,
  no warehouse writes) — e.g. to materialize an old and a new model side-by-side
  for `ol-dbt diff`, to iterate fast on a single changed model, or to bootstrap a
  fresh local warehouse. Covers setup, Glue→DuckDB view registration, incremental
  runs, and safe teardown.
license: BSD-3-Clause
metadata:
  category: data
---

# Local dbt development (DuckDB + Iceberg) with `ol-dbt`

`ol-dbt local` gives you a **zero-copy** local warehouse: production Glue/Iceberg
tables are mounted as DuckDB views, and dbt's `ref()`/`source()` fall back to
those views for models you haven't built. So you can build old + new models
side-by-side against real prod data — without copying it or writing to the
warehouse. The default dbt target for all of this is **`dev_local`** (DuckDB).

## Workflow

### 1. One-time setup
```bash
ol-dbt local setup          # bootstrap the local DuckDB + Iceberg env (installs deps, dbt debug)
```

### 2. Register production tables as DuckDB views
```bash
ol-dbt local register --all-layers      # register all standard dbt layer databases
ol-dbt local register --database ol_warehouse_production_raw   # one layer
ol-dbt local register --force           # re-register everything (default: only new/changed)
ol-dbt local list-sources               # show what's currently registered
```
`register` reads **AWS Glue + S3**, so it needs AWS credentials (unlike the
validation commands, which are fully offline). It is incremental by default and
parallelized; use `--dry-run` to preview.

### 3. Iterate on models
```bash
ol-dbt run                  # incremental: rebuild only changed/errored models (state:modified+ result:error+/fail+ --defer)
ol-dbt run --select my_model+   # build a model and its downstream
ol-dbt run --full-refresh   # full rebuild; also re-initialises state for the next incremental run
```
`ol-dbt run` saves dbt state under `<dbt_project>/.dbt-state/` (`manifest.json`,
`run_results.json`) so subsequent runs only touch what changed — this is the same
slim-CI mechanism used in Phase 2.

### 4. Teardown
```bash
ol-dbt local cleanup-local          # drop locally-registered DuckDB views/tables
ol-dbt local cleanup --dry-run      # preview cleanup of remote dev schemas (namespaced by schema_suffix)
```
Cleanup honors a `PROTECTED_SCHEMAS` guard — it will not drop production or
shared schemas. Always `--dry-run` first when cleaning remote schemas.

## Typical use: materialize both sides for a diff
```bash
ol-dbt local register --all-layers          # mount prod data
ol-dbt run --select dim_user_old dim_user   # build both relations on dev_local
ol-dbt diff --old dim_user_old --new dim_user --primary-key user_pk
```

## Rules

- Default to the **`dev_local`** target — it needs no Trino/warehouse credentials
  and reads Iceberg directly. Only reach for `dev_qa`/`dev_production` targets
  when you specifically need the shared cluster.
- `register` needs AWS creds; `setup`, `run` (on dev_local), and the validation
  commands do not.
- Prefer incremental `ol-dbt run` over `--full-refresh` for fast iteration;
  reserve `--full-refresh` for when the incremental state is stale or wrong.
- Never point `cleanup` at a shared/production schema; rely on `--dry-run` and the
  `PROTECTED_SCHEMAS` guard.
- StarRocks-native `b2b_analytics` models are the exception — they are not
  representable on DuckDB and must be QA'd on StarRocks (see `ol-dbt starrocks`).

Pair this with the `ol-dbt-fast-validation` skill (validate / impact / diff) to
QA what you build. See `docs/specs/DBT_WAREHOUSE_CI_QA_SPEC.md` for the broader
CI/QA plan and the zero-copy substrate details.
