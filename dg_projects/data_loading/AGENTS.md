# data_loading: AI Developer Guide

The `data_loading` code location wraps the standalone **`ol_dlt`** dlt project
(`src/ol_dlt`) and exposes its pipelines as Dagster assets in the
`ol_warehouse_raw_data` schema. This mirrors how `src/ol_dbt` is a standalone dbt
project wrapped by the `lakehouse` code location.

The split:

- **`src/ol_dlt`** owns all *pure dlt* code — sources, resources, and the
  profile-based pipeline/destination configuration. It must **never import
  Dagster** (enforced by a ruff `banned-api` rule).
- **`data_loading`** owns the *orchestration* — thin `@dlt_assets` wrappers,
  translators, schedules, and the edxorg sensor — and imports `ol_dlt`.

See the root [`AGENTS.md`](../../AGENTS.md) for repo-wide build and validation
workflows, and [`src/ol_dlt/README.md`](../../src/ol_dlt/README.md) for the dlt
project itself.

---

## Structure

```
src/ol_dlt/                         # the standalone dlt project (pure dlt)
├── ol_dlt/config.py                # DLT_PROFILE -> destination/dataset/table_format
├── ol_dlt/sources/<name>/__init__.py   # @dlt.source / @dlt.resource bodies
├── .dlt/{config.toml,.pyiceberg.yaml}  # runtime + Glue catalog config
└── tests/

dg_projects/data_loading/
├── data_loading/
│   ├── definitions.py              # load_from_defs_folder + DagsterDltResource
│   └── defs/ingestion/
│       ├── __init__.py             # maps DAGSTER_ENVIRONMENT -> DLT_PROFILE
│       ├── translators.py          # RawDataDltTranslator + EdxorgDltTranslator
│       ├── assets.py               # build_ingest_assets() factory + per-source @dlt_assets
│       ├── schedules.py
│       └── sensor.py               # edxorg upstream multi_asset_sensor
└── data_loading_tests/test_translators.py
```

There is **no** `dlt.yml` / dlt+ project manifest and no per-source `defs.yaml` —
profiles are a plain `DLT_PROFILE` env var resolved in `ol_dlt.config`, and assets
are wired uniformly through the `build_ingest_assets` factory.

---

## Raw Table Naming Convention

**The most common mistake when adding a source. Read before writing any
`@dlt.resource` decorator.**

```
raw__{source_system}__{subsystem}__{storage_technology}__{entity_name}
```

See [`docs/raw_table_naming_conventions.md`](../../docs/raw_table_naming_conventions.md)
for the full reference. Set the name directly on the `@dlt.resource` decorator;
the asset key becomes `["ol_warehouse_raw_data", <resource name>]`.

---

## Adding a New dlt Source (two steps)

### Step 1 — author the source in `src/ol_dlt`

Create `src/ol_dlt/ol_dlt/sources/<name>/__init__.py` (model on
`sources/mit_climate`):

- `@dlt.source(name="<name>_ingest")` and one `@dlt.resource(name="raw__...")`
  per table. **No environment branching, no Dagster imports.**
- `table_format=config.active_table_format()` on each resource (iceberg in
  qa/production, `None` for plain parquet locally — never the string `"parquet"`,
  which dlt rejects).
- Build the pipeline with `config.pipeline_for("<bucket_prefix>")`; the prefix is
  the per-source S3 path segment (`s3://ol-data-lake-raw-<env>/<prefix>`).
- For credentialed sources, resolve secrets **inside the resource generator**
  using `config.resolve_secret(...)` + `config.require_secrets(...)` so the module
  imports cleanly without secrets present.
- Expose a `build_source()` returning the instantiated source (applies any env
  overrides), used by the Dagster wrapper.

Add unit + materialization tests under `src/ol_dlt/tests/sources/` and run
`cd src/ol_dlt && uv run pytest` and `uv run ruff check .` (the banned-api rule
fails the build if `dagster` leaks in).

### Step 2 — wrap it as a Dagster asset

In `dg_projects/data_loading/data_loading/defs/ingestion/assets.py`:

```python
from ol_dlt.sources import <name>

<name>_assets = build_ingest_assets(
    name="<name>_ingest",
    source=<name>.build_source(),
    pipeline=<name>.<name>_pipeline,
)
```

and add `<name>_assets` to the `defs = Definitions(assets=[...])` list. The shared
`RawDataDltTranslator` applies the `ol_warehouse_raw_data` key prefix, the
`ingestion` group, the `dlt` + storage kinds, and the Glue `table_name` metadata.
Sources with upstream Dagster dependencies (like edxorg_s3) get a translator
subclass — see `EdxorgDltTranslator`.

### Validate

```bash
cd dg_projects/data_loading
uv run dg list defs            # the new asset appears under ol_warehouse_raw_data
uv run python -m pytest data_loading_tests/
```

---

## Local Development

```bash
cd src/ol_dlt
# Run a source standalone against the local filesystem destination
DLT_PROFILE=dev uv run python -m ol_dlt.sources.<name>

# Inspect output with DuckDB
duckdb -c "SELECT * FROM read_parquet('/tmp/.dlt/data/<prefix>/**/*.parquet') LIMIT 10"
```

Materialize end-to-end via Dagster from `dg_projects/data_loading` with
`uv run dg dev`.

---

## Secrets

Secrets are **never** stored in `config.toml`. They are provided at runtime via:

- **Kubernetes (production/QA)**: environment variables injected by Helm
- **Local development**: `src/ol_dlt/.dlt/secrets.toml` (gitignored), or exported
  env vars

Required env var names are documented at the top of each source's `__init__.py`
(e.g. `mit_edx_programs` needs `EDX_API_CLIENT_ID`/`EDX_API_CLIENT_SECRET`/
`EDX_API_ACCESS_TOKEN_URL`/`EDX_PROGRAMS_API_URL`).
