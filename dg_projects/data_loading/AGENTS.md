# data_loading: AI Developer Guide

The `data_loading` code location houses all dlt (data load tool) pipelines that
ingest raw data from external sources into the `ol_warehouse_raw_data` Iceberg
schema. This is the primary landing zone for REST API, RSS feed, file export,
and S3-based sources.

See the root [`AGENTS.md`](../../AGENTS.md) for repo-wide build and validation
workflows. This file covers `data_loading`-specific patterns only.

---

## Structure

```
data_loading/
├── data_loading/
│   ├── definitions.py          # Auto-loads all defs/ components + DagsterDltResource
│   └── defs/
│       ├── <source_ingest>/    # One directory per source
│       │   ├── __init__.py
│       │   ├── loads.py        # dlt @dlt.source / @dlt.resource definitions
│       │   ├── defs.yaml       # DltLoadCollectionComponent wiring
│       │   └── README.md       # Source-specific docs and local dev instructions
│       └── ...
├── .dlt/
│   ├── config.toml             # Non-secret pipeline and destination config
│   └── secrets.toml.template   # Template for required secrets (not committed)
└── pyproject.toml
```

`definitions.py` uses `load_from_defs_folder` — adding a new `defs/<source>/`
directory with a valid `defs.yaml` automatically registers its assets; no manual
changes to `definitions.py` are needed.

---

## Raw Table Naming Convention

**This is the most common mistake when adding a new source. Read before writing
any `@dlt.resource` decorator.**

All raw tables must follow:

```
raw__{source_system}__{subsystem}__{storage_technology}__{entity_name}
```

See [`docs/raw_table_naming_conventions.md`](../../docs/raw_table_naming_conventions.md)
for the full reference including:
- When to include `subsystem` vs. when to omit it
- The complete `storage_technology` vocabulary (`api`, `rss`, `postgres`, `google_sheets`, etc.)
- Worked examples for every existing source
- A five-step checklist for naming new tables

Set the name directly on the `@dlt.resource` decorator:

```python
@dlt.resource(
    name="raw__mitpe__api__courses",   # full canonical name — no shortcuts
    primary_key=["title", "url"],
    write_disposition="replace",
)
def courses() -> Generator[dict[str, Any], None, None]:
    ...
```

---

## Adding a New dlt Source

### 1. Create the source directory

```bash
mkdir -p data_loading/defs/<source>_ingest
touch data_loading/defs/<source>_ingest/__init__.py
```

### 2. Write `loads.py`

Follow the pattern in any existing source (e.g., `mit_climate_ingest/loads.py`):

- Decorate the source with `@dlt.source(name="<source>_ingest")`
- Decorate each resource with `@dlt.resource(name="raw__<...>", ...)`
- Use `write_disposition="replace"` for full catalog sources (small, no history needed)
- Use `write_disposition="merge"` with a `primary_key` for incremental sources
- Define module-level `<source>_load_source` and `<source>_pipeline` instances —
  these are referenced by `defs.yaml`
- For sources requiring secrets (OAuth2, API tokens): resolve credentials **inside
  the `@dlt.resource` generator**, not in the outer `@dlt.source` body. The source
  body runs at code location load time; the resource generator runs at pipeline
  execution time. Failing to do this causes load-time errors when secrets are absent
  in local development.

### 3. Write `defs.yaml`

```yaml
type: dagster_dlt.DltLoadCollectionComponent

attributes:
  loads:
    - source: .loads.<source>_load_source
      pipeline: .loads.<source>_pipeline
      translation:
        group_name: ingestion
        key: "{{ resource.name }}"       # required — keeps asset key = table name
        key_prefix: "ol_warehouse_raw_data"
        description: "..."
```

The `key: "{{ resource.name }}"` line is required. Without it, dlt embeds the
pipeline name in the asset key, creating a 3-segment key that diverges from the
2-segment convention used by all other raw assets.

### 4. Add destinations to `.dlt/config.toml`

Add local, QA, and production named destinations for the new pipeline:

```toml
[destination.<source>_local]
destination_type = "filesystem"
bucket_url = "file:///tmp/.dlt/data/<source>"
layout = "{table_name}/{load_id}.{file_id}.{ext}"

[destination.<source>_qa]
destination_type = "filesystem"
bucket_url = "s3://ol-data-lake-raw-qa/<source>"
layout = "{table_name}/{load_id}.{file_id}.{ext}"

[destination.<source>_production]
destination_type = "filesystem"
bucket_url = "s3://ol-data-lake-raw-production/<source>"
layout = "{table_name}/{load_id}.{file_id}.{ext}"
```

And the corresponding Iceberg catalog entries for QA and production (see existing
entries in `config.toml` for the pattern).

### 5. Validate the code location loads

```bash
cd dg_projects/data_loading
uv run python -c "from data_loading.definitions import defs; print('OK')"
```

This must succeed with no errors before opening a PR. A failure here means either
the `defs.yaml` references a missing attribute, or a module-level secret resolution
is happening too early (see step 2 note above).

---

## Local Development

Run a pipeline locally against the filesystem destination:

```bash
cd dg_projects/data_loading

# Run standalone (writes Parquet to /tmp/.dlt/data/<source>/)
uv run python -m data_loading.defs.<source>_ingest.loads

# Inspect output with DuckDB
duckdb -c "SELECT * FROM read_parquet('/tmp/.dlt/data/raw__<source>__*/**/*.parquet') LIMIT 10"
```

---

## Secrets

Secrets are **never** stored in `config.toml`. They are provided at runtime via:

- **Kubernetes (production/QA)**: environment variables injected by Helm
- **Local development**: create `.dlt/secrets.toml` (gitignored) using
  `.dlt/secrets.toml.template` as a guide

For OAuth2 sources (e.g., `mit_edx_programs_ingest`), the required env var names
are documented at the top of each `loads.py` file.
