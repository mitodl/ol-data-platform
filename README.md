# Overview

This repository holds the business logic for building and managing the data pipelines used to power various data
services at MIT Open Learning. The core framework is Dagster which provides a flexible, and well structured approach to
building data applications.

# Running Dagster Locally via Docker
- Ensure that you have the latest version of Docker installed.
    https://www.docker.com/products/docker-desktop/
- Install docker compose. Check the documentation and requirements for your specific machine.
    https://docs.docker.com/compose/install/
- Ensure you are able to authenticate into GitHub + Vault
    https://github.com/mitodl/ol-data-platform/tree/main
    https://vault-qa.odl.mit.edu/v1/auth/github/login
    `vault login -address=https://vault-qa.odl.mit.edu -method=github`
    https://vault-production.odl.mit.edu/v1/auth/github/login
    `vault login -address=https://vault-production.odl.mit.edu -method=github`
- Ensure you create your .env file and populate it with the environment variables.
    `cp .env.example .env`
- Call docker compose up
    `docker compose up --build`
- Navigate to localhost:3000 to access the Dagster UI

# dbt Staging Model Generation

This repository includes a script for automatically generating dbt source definitions and staging models from database tables. The script is located at `bin/dbt-create-staging-models.py`.

## Prerequisites

- Python environment with required dependencies (see `pyproject.toml`)
- dbt environment configured with appropriate credentials
- Access to the target database/warehouse

## Usage

The script provides three main commands:

### 1. Generate Sources Only

```bash
python bin/dbt-create-staging-models.py generate-sources \
    --schema ol_warehouse_production_raw \
    --prefix raw__mitlearn__app__postgres__user \
    --target production
```

### 2. Generate Staging Models Only

```bash
python bin/dbt-create-staging-models.py generate-staging-models \
    --schema ol_warehouse_production_raw \
    --prefix raw__mitlearn__app__postgres__user \
    --target production
```

### 3. Generate Both Sources and Staging Models

```bash
python bin/dbt-create-staging-models.py generate-all \
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
python bin/dbt-create-staging-models.py generate-all \
    --schema ol_warehouse_production_raw \
    --prefix raw__mitlearn__app__postgres__user \
    --target production
```

### Generate Without Transformations (Legacy Mode)
```bash
python bin/dbt-create-staging-models.py generate-all \
    --schema ol_warehouse_production_raw \
    --prefix raw__mitlearn__app__postgres__user \
    --target production \
    --no-apply-transformations
```

### Override Entity Type Detection
```bash
python bin/dbt-create-staging-models.py generate-all \
    --schema ol_warehouse_production_raw \
    --prefix raw__mitlearn__app__postgres__user \
    --target production \
    --entity-type user
```

### Basic Generate MITlearn User Tables
```bash
python bin/dbt-create-staging-models.py generate-all \
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
python bin/dbt-create-staging-models.py generate-sources \
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
