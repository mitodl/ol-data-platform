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

`bin/dagster-up` authenticates to Vault for you and then runs `docker compose up
--build`. Vault uses the Keycloak OIDC browser flow, so there is no token to
create or paste into `.env`.

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

If a code location fails to start with "No valid cached Vault token", the token
expired — run `bin/vault-login` and restart.

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

# UV Operations Utility

This repository includes a utility script for running `uv` commands across all code locations in the `dg_deployment/code_locations` directory. The script is located at `bin/uv-operations.py`.

## Overview

The `uv-operations.py` script automatically discovers all directories containing a `pyproject.toml` file in the code locations directory and executes the specified `uv` command on each one. This is useful for operations like:

- Synchronizing dependencies across all code locations (`uv sync`)
- Upgrading lock files (`uv lock --upgrade`)
- Building packages (`uv build`)
- Listing installed packages (`uv pip list`)

## Usage

### Basic Command

```bash
python bin/uv-operations.py <uv-command> [args...]
```

Or run it directly as an executable:

```bash
./bin/uv-operations.py <uv-command> [args...]
```

### Examples

#### Sync all code locations

```bash
python bin/uv-operations.py sync
```

#### Upgrade lock files

```bash
python bin/uv-operations.py lock --upgrade
```

#### List packages in all locations

```bash
python bin/uv-operations.py pip list
```

#### Continue on errors

By default, the script stops at the first failure. To continue processing all locations even if some fail:

```bash
python bin/uv-operations.py sync --continue-on-error
```

#### Verbose output

For detailed output showing the exact commands being run:

```bash
python bin/uv-operations.py sync --verbose
```

## Parameters

- `--code-locations-dir`: Base directory containing code locations (default: `dg_deployment/code_locations`)
- `--continue-on-error`: Continue running even if some locations fail
- `--verbose`: Print verbose output including the full command being executed

## Output

The script provides:

- A list of discovered code locations
- Progress indicators for each location being processed
- Success (✓) or failure (✗) markers for each location
- A summary at the end showing successful and failed operations
