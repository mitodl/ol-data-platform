# ol-data-platform: AI Developer Guide

**Critical: Read this entire document before making changes. It contains validated build/test workflows and common error solutions that will save significant time.**

This guide provides essential information for coding agents working with the MIT Open Learning data platform. It covers architectural patterns, build processes, validation workflows, and common errors with workarounds.

## Repository Overview

**Purpose**: Data orchestration platform for MIT Open Learning that ingests, transforms, and delivers educational data from Canvas, Open edX, and other sources to analytics tools and dashboards.

**Repository Type**: Python uv workspace with 7+ Dagster code locations and a dbt project
**Size**: ~209 Python dependencies, ~30 workspace projects
**Python Version**: 3.13 (strictly ~=3.13,<3.14)
**Package Manager**: uv 0.9+ (replaces pip/poetry)

**Key Technologies**:


- **Orchestration**: [Dagster](https://dagster.io/) 1.11+ for asset-centric data pipelines
- **Transformation**: [dbt](https://www.getdbt.com/) 1.10+ with Trino adapter for SQL transformations
- **Storage**: S3-based data lakehouse on AWS
- **Query Engine**: Trino via Starburst Galaxy
- **BI & Visualization**: Apache Superset
- **Secrets Management**: HashiCorp Vault (never hardcode secrets)

## Build & Validation Workflow

**CRITICAL: Follow these exact steps to avoid CI failures. Commands tested and validated.**

### Initial Setup (One-Time)

```bash
# 1. Copy environment template
cp .env.example .env
# Edit .env with: GITHUB_TOKEN, DBT_TRINO_USERNAME, DBT_TRINO_PASSWORD, AWS keys, DBT_SCHEMA_SUFFIX

# 2. Sync dependencies (10-30 seconds)
uv sync

# 3. Install pre-commit hooks
pre-commit install
```

### Making Code Changes - Critical Sequence

**For Python changes** (always run in this order):
```bash
# 1. Format code (fast, 2-5 sec)
ruff format .

# 2. Auto-fix linting issues (5-10 sec)
ruff check --fix .

# 3. Type check (10-15 sec)
mypy packages/ dg_projects/ --config-file=pyproject.toml

# 4. Run pre-commit (IMPORTANT: sqlfluff can take 60+ seconds)
pre-commit run --all-files
```

**For dbt SQL changes**:
```bash
cd src/ol_dbt

# 1. Install dbt packages (3-5 sec)
dbt deps

# 2. Format SQL
sqlfmt models/

# 3. Compile to verify (10-20 sec)
dbt compile -t dev

# 4. Run specific model
dbt run --select my_model_name -t dev

# 5. Test
dbt test --select my_model_name -t dev
```

### Pre-Commit Validation

**Before every commit, run**:
```bash
pre-commit run --all-files
```

**Pre-commit hooks in order** (all must pass for CI):
1. trailing-whitespace, end-of-file-fixer, check-yaml, check-toml
2. yamlfmt, yamllint
3. detect-secrets (scans for leaked credentials)
4. ruff-format, ruff (Python linting)
5. mypy (type checking)
6. sqlfluff-fix, sqlfluff-lint (SQL - SLOWEST, 60+ seconds)

**Skip slow hooks during iteration**:
```bash
SKIP=sqlfluff-lint pre-commit run --all-files
```

### Common Errors & Solutions

**Error: "uv sync package conflicts"**
→ Solution: `uv lock --upgrade && uv sync`

**Error: "dbt deps fails" / "dbt not found"**
→ Solution: Ensure in `src/ol_dbt/` directory; run `uv sync` first

**Error: "pre-commit sqlfluff timeout"**
→ Solution: sqlfluff-lint is slow (60+ sec). Run on specific files or skip: `SKIP=sqlfluff-lint pre-commit run`

**Error: "mypy import errors"**
→ Solution: Ensure `uv sync` completed; check `[tool.mypy]` has `ignore_missing_imports = true`

**Error: "Docker build fails"**
→ Solution: Verify .env configured; Docker daemon running; `docker compose up --build` (2-5 min first time)

**Error: "Asset not found in Dagster"**
→ Solution: Verify asset has `group_name` set; check it's imported in definitions.py

### Utility Scripts

**Generate dbt staging models automatically**:
```bash
uv run python bin/dbt-create-staging-models.py generate-all \
  --schema ol_warehouse_production_raw \
  --prefix raw__mitlearn__app__postgres__user \
  --target production
```

**Run uv commands across all projects**:
```bash
python bin/uv-operations.py sync --continue-on-error
python bin/uv-operations.py lock --upgrade
```

## Directory Structure & Key Files


**Critical Paths**:
- `packages/ol-orchestrate-lib/src/ol_orchestrate/`: Shared library used by all Dagster code locations
  - `resources/`: Reusable connections (databases, APIs, Vault)
  - `io_managers/`: S3FileObjectIOManager, FileObjectIOManager for asset persistence
  - `lib/`: Utilities, constants, helpers
- `dg_projects/`: 7 Dagster code locations (canvas, data_platform, edxorg, lakehouse, learning_resources, legacy_openedx, openedx)
  - Each has: `<project>/assets/`, `<project>/resources/`, `<project>/definitions.py`, `pyproject.toml`, `Dockerfile`
- `dg_deployments/local/`: Local Docker deployment configuration
- `src/ol_dbt/`: Complete dbt project
  - `models/staging/`: Raw data 1:1 transformations
  - `models/intermediate/`: Business logic, joins
  - `models/marts/`: Analytics-ready tables for BI
  - `dbt_project.yml`, `profiles.yml`, `packages.yml`
- `bin/`: Utility scripts (dbt-create-staging-models.py, uv-operations.py)

**Configuration Files** (IMPORTANT):
- Root `pyproject.toml`: Workspace configuration, uv settings, tool config (ruff, mypy, sqlfluff)
- `.pre-commit-config.yaml`: All pre-commit hooks (must pass for CI)
- `docker-compose.yaml`: Local dev environment (7 code location services + webserver + daemon)
- `.env.example`: Template for required environment variables
- `src/ol_dbt/dbt_project.yml`: dbt project configuration

**GitHub Workflows**:
- `.github/workflows/publish_dbt_docs.yaml`: Publishes dbt docs to GitHub Pages on main push
- `.github/workflows/project_automation.yaml`: Adds issues to project board

## Dagster Architecture & Conventions


### Workspace Structure (CRITICAL)

This is a **uv workspace** with unique dependency management:
- Root `pyproject.toml` defines workspace members: `members = ["packages/*"]`
- `packages/ol-orchestrate-lib/` is an editable shared package
- Each `dg_projects/<project>/pyproject.toml` references ol-orchestrate-lib:
  ```toml
  [tool.uv.sources]
  ol-orchestrate-lib = { path = "../../packages/ol-orchestrate-lib", editable = true }
  ```
- **Impact**: Changes to ol-orchestrate-lib affect ALL code locations

### Asset-Centric Development (MANDATORY PATTERN)


**Requirements**:
-   Define all data as assets using `@asset`, `@multi_asset`, or `@dbt_assets`
-   Every asset MUST have a `group_name` (e.g., "canvas", "openedx", "lakehouse")
-   Use structured `AssetKey(["domain", "subdomain", "name"])` for namespacing
-   Assets represent persistent objects (table in lake, file in S3)

**Example**:
```python
@asset(
    group_name="openedx",
    io_manager_key="s3file_io_manager"
)
def course_xml_data(context):
    # Asset implementation
    return data
```

### Resource Pattern (MANDATORY FOR EXTERNAL SERVICES)


**Rules**:
-   All external interactions (DBs, APIs, storage) MUST use a Dagster `Resource`
-   Define in `packages/ol-orchestrate-lib/src/ol_orchestrate/resources/` using `ConfigurableResource`
-   API clients use `...Factory` pattern (see `CanvasApiClientFactory`, `OpenEdxApiClientFactory`)
-   Configuration varies by `DAGSTER_ENV` (dev/qa/production)
-   **Secrets ALWAYS from Vault** (never hardcode)

**Example Resource**:
```python
class MyApiFactory(ConfigurableResource):
    vault: VaultResource

    def get_client(self) -> MyApiClient:
        secret = self.vault.get_secret("path/to/secret")
        return MyApiClient(token=secret["token"])
```

### IO Management


-   Primary: `S3FileObjectIOManager` and `FileObjectIOManager` (write to S3)
-   Specify `io_manager_key` on assets if non-default needed
-   Handles serialization/deserialization automatically

## dbt Integration


### dbt Model Organization

**Layer Structure** (strict separation):
- `models/staging/`: 1:1 with source tables, naming: `stg_<source>__<table>.sql`
- `models/intermediate/`: Business logic, joins, naming: `int_<domain>__<description>.sql`
- `models/marts/`: Analytics-ready, naming: `fct_<domain>__<metric>.sql` or `dim_<domain>__<entity>.sql`

**To add a dbt model**:
1. Place `.sql` in appropriate directory
2. Run `dbt compile -t dev` (verify no errors)
3. Run `dbt run --select my_model -t dev`
4. Model automatically appears in Dagster asset graph (via `@dbt_assets`)

### Airbyte Ingestion

-   [Airbyte](https://airbyte.com/) extracts/loads to S3 raw layer
-   Auto-loaded as Dagster source assets via `load_assets_from_airbyte_instance`
-   Uses `key_prefix` `ol_warehouse_raw_data` to map to dbt sources

### Superset Integration


-   Automated dataset creation/refresh in Apache Superset
-   `create_superset_asset` generates Dagster asset for each dbt model
-   When dbt model updates, triggers API call to refresh Superset dataset
-   **To expose model**: Add parent folder to `dbt_models_for_superset_datasets` in `dg_projects/lakehouse/lakehouse/defs/lakehouse_datasets.py`

## Adding New Components

### Add New API Data Source

1. **Resource**: Create `ConfigurableResource` in `packages/ol-orchestrate-lib/src/ol_orchestrate/resources/`
2. **Assets**: Create file in `dg_projects/<project>/<project>/assets/my_source.py`
3. **Define Assets**: Use resource (`required_resource_keys={"my_api"}`), yield `Output`
4. **IO Manager**: Set `io_manager_key="s3file_io_manager"`
5. **Register**: Import in `dg_projects/<project>/<project>/definitions.py`
6. **Validate**: Run `dagster dev` locally to verify asset appears

### Add New dbt Model

1. **Create**: Add `.sql` in `src/ol_dbt/models/<layer>/`
2. **Sources**: If new raw data, add to `_<domain>__sources.yml`
3. **Compile**: `cd src/ol_dbt && dbt compile -t dev`
4. **Test**: `dbt run --select my_model -t dev`
5. **Verify**: Check `src/ol_dbt/target/compiled/` for compiled SQL
6. **Auto-integration**: Dagster `@dbt_assets` auto-discovers (no Python changes needed)
7. **Superset** (optional): Add parent dir to `dbt_models_for_superset_datasets`

## Environment Variables

**Required in `.env`** (from `.env.example`):
- `GITHUB_TOKEN`: GitHub PAT for auth
- `DBT_TRINO_USERNAME`, `DBT_TRINO_PASSWORD`: Starburst credentials
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`: S3 access
- `DBT_SCHEMA_SUFFIX`: Your dev schema suffix (e.g., username)

**Runtime**:
- `DAGSTER_ENV`: dev|qa|production (selects resource config)

## Trust These Instructions

These instructions are comprehensive, tested, and validated. Only search for additional information if:
1. A command fails with an error not documented here
2. You need implementation details for a specific asset or model
3. The repository structure has changed significantly

**Version**: Validated against Python 3.13, uv 0.9.3, Dagster 1.11, dbt 1.10 (Oct 2025)
