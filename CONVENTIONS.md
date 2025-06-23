# Project Conventions for AI-Assisted Development

This document outlines the architectural patterns, conventions, and best practices used
in the `ol-data-platform` repository. It is intended to be used as a reference for
developers and as a guide for AI coding assistants to ensure that new contributions are
consistent with the existing codebase.

## 1. Overview

This project orchestrates data ingestion, transformation, and delivery for MIT Open
Learning.

- **Orchestration**: [Dagster](https://dagster.io/) is used to define and manage data
  pipelines as a series of versioned, typed assets.
- **Transformation**: [dbt](https://www.getdbt.com/) is used for SQL-based data
  transformations within the data warehouse.
- **Storage**: The primary data store is a data lakehouse built on **AWS S3**.
- **Query Engine**: **Trino** (via Starburst Galaxy) is the query engine used to
  interact with the data lakehouse.
- **BI & Visualization**: **Apache Superset** is the primary interface for data
  exploration and dashboarding.
- **Secrets Management**: **HashiCorp Vault** is used for securely storing and
  retrieving secrets like API keys and passwords.

## 2. Directory Structure

The core orchestration logic resides in the `src/ol_orchestrate/` directory.

-   `assets/`: Contains Dagster asset definitions. Each file or subdirectory typically
    corresponds to a data source or domain (e.g., `canvas.py`, `openedx.py`,
    `lakehouse/dbt.py`).
-   `definitions/`: Contains Dagster `Definitions` objects. These objects bundle assets,
    resources, jobs, and schedules into deployable units of code.
-   `resources/`: Contains definitions for Dagster `Resources`. Resources are reusable
    components that connect to external services like databases (Postgres, MySQL), APIs
    (Canvas, Open edX), and storage (S3, GCS).
-   `io_managers/`: Contains custom IO Managers that handle the storage and retrieval of
    asset outputs, primarily between S3 and the Dagster environment.
-   `lib/`: A collection of shared utilities, constants, helper functions, and custom
    types used across the project.
-   `partitions/`, `schedules/`, `sensors/`: Definitions for Dagster's scheduling,
    partitioning, and event-driven sensor logic.
-   `ol_dbt/`: (Located at the repository root) The complete dbt project, containing all
    SQL models, tests, and configurations.

## 3. Dagster Conventions

### Asset-Centric Development

The primary paradigm for data pipeline development is the **asset-centric model**.

-   **What to do**: Define all data dependencies as a graph of assets using `@asset`,
    `@multi_asset`, or `@dbt_assets`. An asset should represent a persistent object in
    storage (e.g., a table in the data lake, a file in S3).
-   **Asset Naming**: Use descriptive names for assets. `AssetKey`s should be structured
    lists of strings, forming a logical path (e.g., `AssetKey(["openedx", "raw_data",
    "course_xml"])`).
-   **Grouping**: Assign every asset to a `group_name`. This is crucial for organization
    in the Dagster UI. The group name typically corresponds to the source system or
    logical data domain (e.g., `"openedx"`, `"canvas"`, `"superset_dataset"`).

### Resource Abstraction for External Services

All interactions with external systems (databases, APIs, file systems) **must** be
mediated through a Dagster `Resource`.

-   **What to do**: Define resources in the `src/ol_orchestrate/resources/` directory
    using `ConfigurableResource`. This pattern centralizes configuration and makes
    assets more testable.
-   **API Clients**: For API interactions, use the `...Factory` pattern seen in
    `CanvasApiClientFactory` and `OpenEdxApiClientFactory`. The factory resource handles
    fetching credentials from Vault and initializing the actual API client, which is
    then exposed to the asset.
-   **Configuration**: Resource configuration is managed per-environment. The
    `DAGSTER_ENV` environment variable (`dev`, `qa`, `production`) is used to select the
    correct configuration (e.g., hostnames, database names).
-   **Secrets**: All secrets (tokens, passwords, keys) **must** be fetched from
    HashiCorp Vault via the `Vault` resource. Do not hardcode secrets.

### IO Management

-   Asset outputs are managed by IO Managers, which handle serialization and
    persistence.
-   The primary IO managers are `S3FileObjectIOManager` and `FileObjectIOManager`, which
    write outputs to AWS S3.
-   When defining assets, specify an `io_manager_key` if a non-default manager is
    needed.

## 4. dbt and Airbyte Integration

### Airbyte for Ingestion

-   [Airbyte](https://airbyte.com/) is used for ELT, extracting data from source systems
    and loading it into the raw layer of the S3 data lake.
-   Airbyte connections are automatically loaded into Dagster as source assets using
    `load_assets_from_airbyte_instance`.
-   The `key_prefix` `ol_warehouse_raw_data` is used to map Airbyte outputs to dbt
    sources.

### dbt for Transformation

-   All SQL transformations are defined as models within the `ol_dbt/` project.
-   These models are brought into the Dagster asset graph using the `@dbt_assets`
    decorator in `src/ol_orchestrate/assets/lakehouse/dbt.py`.
-   A custom `DbtAutomationTranslator` is used to control how dbt models are represented
    as assets in Dagster.

## 5. Superset Integration

-   A key feature of this repository is the automated creation and refreshing of
    datasets in Apache Superset.
-   The `create_superset_asset` function in `src/ol_orchestrate/assets/superset.py`
    generates a Dagster asset for a corresponding dbt model.
-   This asset depends on the dbt model asset. When the dbt model is updated, the
    Superset asset runs, triggering an API call to refresh the dataset in Superset.
-   To expose a new dbt model in Superset, add its parent folder name to the
    `dbt_models_for_superset_datasets` set in
    `src/ol_orchestrate/definitions/lakehouse/elt.py`.

## 6. Adding New Components

### How to Add a New API-based Data Source

1.  **Resource**: Create a new `ConfigurableResource` in `src/ol_orchestrate/resources/`
    for the new API. Follow the factory pattern if authentication is complex. Add any
    required secrets to Vault.
2.  **Assets**: Create a new file in `src/ol_orchestrate/assets/` (e.g.,
    `my_new_source.py`).
3.  **Define Assets**: In the new file, define assets that use your new resource
    (`required_resource_keys={"my_new_source_api"}`). These assets should perform the
    API calls and yield `Output` objects.
4.  **IO Manager**: Assign an `io_manager_key` (e.g., `"s3file_io_manager"`) to your
    assets to ensure their outputs are stored in S3.
5.  **Definitions**: Add your new assets to a relevant `Definitions` object in
    `src/ol_orchestrate/definitions/`.

### How to Add a New dbt Model

1.  **Create Model**: Add your `.sql` file to the appropriate subdirectory within
    `ol_dbt/models/`.
2.  **Define Sources**: If the model depends on new raw data, define the source in a
    `sources.yml` file. The source tables should match the table names produced by
    Airbyte.
3.  **Run dbt**: Run `dbt build` locally to test your changes.
4.  **Dagster Integration**: The `@dbt_assets` definition will automatically discover
    and load your new model into the Dagster asset graph. No Python code changes are
    typically needed.
5.  **(Optional) Expose to Superset**: If the model should be a dataset in Superset, add
    its parent directory name (e.g., `"mart"`) to the `dbt_models_for_superset_datasets`
    set in `src/ol_orchestrate/definitions/lakehouse/elt.py`.
