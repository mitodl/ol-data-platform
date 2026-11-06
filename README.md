# Overview

This repository holds the business logic for building and managing the data pipelines used to power various data
services at MIT Open Learning. The core framework is Dagster which provides a flexible, and well structured approach to
building data applications.

# Running Dagster Locally via Docker
- Ensure that you have the latest version of Docker installed.
    https://www.docker.com/products/docker-desktop/
- Install docker compose. Check the documentation and requirements for your specific machine.
    https://docs.docker.com/compose/install/
- Ensure you create your .env file and populate it with the environment variables.
    `cp .env.example .env`
- Start the stack
    `bin/dagster-up`
- Navigate to localhost:3000 to access the Dagster UI

`bin/dagster-up` authenticates to Vault for you and then runs
`docker compose up --build`. Vault uses the Keycloak OIDC browser flow, so there
is no token to create or paste into `.env`.

The containers cannot open a browser or receive the OIDC redirect on
`localhost:8250`, so the login happens on the host: `bin/vault-login` caches a
short-lived token in `~/.cache/vault`, and the code-location containers mount
that directory read-only. One login is shared by every container and is reused
until it expires — re-running is a no-op while the token is still valid.

To authenticate by hand, or against production:

```bash
bin/vault-login                  # qa, which is what local dev targets
bin/vault-login --env production
bin/vault-login --force          # discard the cached token and re-authenticate
```

If the token expires, code locations warn `Failed to authenticate with Vault: No
valid cached Vault token ... Using mock configuration` and load against mock
config rather than real secrets. `edxorg` is the exception — it reads a Vault
secret at import time, so it fails to start outright. Either way the fix is to
run `bin/vault-login` and restart the stack.

`bin/vault-login` and the containers agree on the cache location via
`VAULT_TOKEN_CACHE_DIR`; set it on the host to move the cache, and compose
mounts whatever it points at.

# dbt Staging Model Generation

The `ol-dbt` CLI provides commands for automatically generating dbt source definitions and staging models from database tables. Install it with `uv sync` and then run `ol-dbt generate --help`.

## Prerequisites

- Python environment with required dependencies (see `pyproject.toml`)
- dbt environment configured with appropriate credentials
- Access to the target database/warehouse

## Usage

The script provides three main commands:

### 1. Generate Sources Only

```bash
ol-dbt generate sources \
    --schema ol_warehouse_production_raw \
    --prefix raw__mitlearn__app__postgres__user \
    --target production
```

### 2. Generate Staging Models Only

```bash
ol-dbt generate staging-models \
    --schema ol_warehouse_production_raw \
    --prefix raw__mitlearn__app__postgres__user \
    --target production
```

### 3. Generate Both Sources and Staging Models

```bash
ol-dbt generate all \
    --schema ol_warehouse_production_raw \
    --prefix raw__mitlearn__app__postgres__user \
    --target production
```

## Parameters

- `--schema`: The database schema to scan for tables (e.g., `ol_warehouse_production_raw`)
- `--prefix`: The table prefix to filter by (e.g., `raw__mitlearn__app__postgres__user`)
- `--target`: The dbt target environment to use (`production`, `qa`, `dev`, etc.)
- `--database`: (Optional) Specify the database name if different from target default
- `--directory`: (Optional) Override the subdirectory within `models/staging/`
- `--apply-transformations`: (Optional) Apply semantic transformations (default: True)
- `--entity-type`: (Optional) Override auto-detection of entity type (user, course, courserun, etc.)

## How It Works

1. **Domain Detection**: Extracts the domain from the prefix (e.g., `mitlearn` from `raw__mitlearn__app__postgres__`)
2. **Entity Detection**: Automatically detects entity type from table name for semantic transformations
3. **File Organization**: Creates files in `src/ol_dbt/models/staging/{domain}/`
4. **Source Generation**: Uses dbt-codegen to discover matching tables and generate source definitions
5. **Enhanced Staging Models**: Creates SQL and YAML files with automatic transformations applied
6. **Merging**: Automatically merges new tables with existing source files

## Enhanced Staging Model Generation

The script now includes an enhanced macro that automatically applies common transformation patterns:

### Automatic Transformations
- **Semantic Column Renaming**: `id` → `{entity}_id`, `title` → `{entity}_title`
- **Timestamp Standardization**: Converts all timestamps to ISO8601 format
- **Boolean Normalization**: Ensures consistent boolean field naming
- **Data Quality**: Automatic deduplication for Airbyte sync issues
- **String Cleaning**: Handles multiple spaces in user names

### Entity Type Detection
The system auto-detects entity types from table names:
- `user` tables → User-specific transformations
- `course` tables → Course-specific transformations
- `courserun` tables → Course run transformations
- `video`, `program`, `website` → Respective entity transformations
2. **File Organization**: Creates files in `src/ol_dbt/models/staging/{domain}/`
3. **Source Generation**: Uses dbt-codegen to discover matching tables and generate source definitions
4. **Staging Models**: Creates SQL and YAML files for each discovered table
5. **Merging**: Automatically merges new tables with existing source files

## Generated Files

### Sources File
- **Location**: `src/ol_dbt/models/staging/{domain}/_{domain}__sources.yml`
- **Format**: Standard dbt sources configuration with dynamic schema references
- **Merging**: Automatically merges with existing source definitions

### Staging Models
- **SQL Files**: `stg_{domain}__{table_name}.sql` - Generated base models with enhanced transformations and explicit column selections
- **YAML File**: `_stg_{domain}__models.yml` - Consolidated model schema definitions for all staging models in the domain

## Examples

### Generate MITlearn User Tables with Enhanced Transformations
```bash
ol-dbt generate all \
    --schema ol_warehouse_production_raw \
    --prefix raw__mitlearn__app__postgres__user \
    --target production
```

### Generate Without Transformations (Legacy Mode)
```bash
ol-dbt generate all \
    --schema ol_warehouse_production_raw \
    --prefix raw__mitlearn__app__postgres__user \
    --target production \
    --no-apply-transformations
```

### Override Entity Type Detection
```bash
ol-dbt generate all \
    --schema ol_warehouse_production_raw \
    --prefix raw__mitlearn__app__postgres__user \
    --target production \
    --entity-type user
```

### Basic Generate MITlearn User Tables
```bash
ol-dbt generate all \
    --schema ol_warehouse_production_raw \
    --prefix raw__mitlearn__app__postgres__user \
    --target production
```

This creates:
- `src/ol_dbt/models/staging/mitlearn/_mitlearn__sources.yml` - Source definitions
- `src/ol_dbt/models/staging/mitlearn/_stg_mitlearn__models.yml` - Consolidated model definitions
- `src/ol_dbt/models/staging/mitlearn/stg_mitlearn__raw__mitlearn__app__postgres__users_user.sql` - Individual SQL files
- Additional SQL files for other discovered user-related tables

### Add Additional Tables to Existing Sources
```bash
ol-dbt generate sources \
    --schema ol_warehouse_production_raw \
    --prefix raw__mitlearn__app__postgres__auth \
    --target production
```

This merges auth-related tables into the existing `_mitlearn__sources.yml` file.

## Notes

- The script follows existing dbt project conventions and naming patterns
- Source files use the standard `ol_warehouse_raw_data` source with dynamic schema configuration
- Generated staging models reference the correct source and include all discovered columns
- The script handles YAML merging to avoid duplicating source definitions

# uv lockfiles

The root workspace (`packages/*`, `src/ol_dbt_cli`) shares `uv.lock`, but every
other uv project has its own: each code location under `dg_projects/`, the two
deployments under `dg_deployments/`, `src/ol_dlt` and `src/ol_superset`. Each of
those lockfiles also records the dependencies of its path dependencies, so a
change to `packages/ol-orchestrate-lib/pyproject.toml` makes all of them stale,
and the image builds (`uv sync --locked`) fail until they are relocked.

`bin/uv-operations.py` runs a uv command in each of these projects. It finds them
as every `pyproject.toml` tracked in git that is not a member of the root
workspace, so a new project is covered once it is committed.

```bash
uv run bin/uv-operations.py list                           # the projects it covers
uv run bin/uv-operations.py lock                           # relock whatever is stale
uv run bin/uv-operations.py lock --check                   # fail if anything is stale
uv run bin/uv-operations.py lock --upgrade-package dagster # upgrade one package everywhere
uv run bin/uv-operations.py lock --upgrade                 # upgrade everything everywhere
uv run bin/uv-operations.py sync
uv run bin/uv-operations.py exec pip list                  # any other uv command
```

It runs every project and reports the failures at the end; `--fail-fast` stops at
the first one.

The `uv-lock` prek hook runs `lock` whenever a `pyproject.toml` or `uv.lock`
changes, and autofix.ci runs it on every pull request, so a dependency bump that
edits one `pyproject.toml` gets the other lockfiles relocked in the same PR.
