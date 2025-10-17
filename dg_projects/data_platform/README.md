# Data Platform Code Location

This Dagster code location provides platform-level functionality for the MIT Open Learning data platform, including:

- **Slack notifications** for run failures across all repositories
- **OpenMetadata integration** for metadata ingestion and data governance

## OpenMetadata Integration

The OpenMetadata integration ingests metadata, lineage, and data profiling information from various data sources into OpenMetadata for improved data discovery and governance.

### Supported Data Sources

The following data sources are configured for metadata ingestion:

1. **Trino (Starburst Galaxy)** - Database metadata and lineage
2. **dbt** - Model definitions, documentation, and tests
3. **Dagster** - Pipeline definitions and assets
4. **Redash** - Query and dashboard definitions
5. **Superset** - Dashboard, chart, and dataset definitions
6. **S3** - Bucket and object structure
7. **Iceberg** - Table metadata, schemas, partitioning, and profiling
8. **Airbyte** - Connection and sync information

### Assets

Each data source has one or more assets defined in `data_platform/assets/metadata/ingestion.py`:

- `openmetadata__trino__metadata` - Trino table schemas and database structure
- `openmetadata__trino__lineage` - Data lineage from Trino query logs
- `openmetadata__trino__profiling` - Statistical profiling of Trino tables
- `openmetadata__dbt__metadata` - dbt model metadata
- `openmetadata__dbt__lineage` - dbt model lineage and dependencies
- `openmetadata__dagster__metadata` - Dagster pipeline metadata
- `openmetadata__superset__metadata` - Superset dashboard metadata
- `openmetadata__airbyte__metadata` - Airbyte connection metadata
- `openmetadata__s3__metadata` - S3 bucket and object metadata
- `openmetadata__iceberg__metadata` - Iceberg table metadata
- `openmetadata__iceberg__profiling` - Statistical profiling of Iceberg tables
- `openmetadata__redash__metadata` - Redash query and dashboard metadata

### Schedules

Two schedules are defined for regular metadata updates:

1. **metadata_ingestion_schedule** - Daily at 2 AM, ingests metadata from all sources
2. **critical_metadata_schedule** - Every 4 hours, ingests metadata from Trino, dbt, and Dagster

Both schedules are stopped by default and should be enabled in production.

## Configuration

### Environment Variables

The OpenMetadata client is configured based on the `DAGSTER_ENV` environment variable:

- `dev`: `http://localhost:8585/api`
- `qa`: `https://openmetadata-qa.odl.mit.edu/api`
- `production`: `https://openmetadata.odl.mit.edu/api`

### Vault Secrets

The following secrets must be configured in HashiCorp Vault:

- **Path**: `secret-data/dagster/openmetadata`
- **Required fields**:
  - `jwt_token` - JWT token for OpenMetadata authentication

### Data Source Configuration

Each data source asset contains configuration that may need to be updated:

- **Trino**: Update `hostPort`, `catalog`, and `databaseSchema` as needed
- **dbt**: Update file paths to point to dbt artifacts (catalog.json, manifest.json, run_results.json)
- **Dagster**: Update `host` and `port` for Dagster webserver
- **Superset**: Update `hostPort` for Superset instance
- **Airbyte**: Update `hostPort` for Airbyte instance
- **S3**: Update `awsRegion` and bucket filter patterns
- **Iceberg**: Update `awsRegion` and schema filter patterns
- **Redash**: Update `hostPort` for Redash instance

## Usage

### Running Metadata Ingestion

To manually trigger metadata ingestion:

```bash
# Materialize all metadata assets
dagster asset materialize -m data_platform.definitions --select "openmetadata/*"

# Materialize specific source
dagster asset materialize -m data_platform.definitions --select "openmetadata__trino__metadata"
```

### Enabling Schedules

Schedules can be enabled in the Dagster UI:

1. Navigate to "Schedules" in the Dagster UI
2. Find `metadata_ingestion_schedule` or `critical_metadata_schedule`
3. Click "Start Schedule"

### Viewing Metadata in OpenMetadata

After ingestion, metadata can be viewed in the OpenMetadata UI at the configured URL.

## Development

### Adding a New Data Source

To add a new data source:

1. Create a new asset function in `data_platform/assets/metadata/ingestion.py`
2. Define the workflow configuration following the OpenMetadata connector documentation
3. Add the asset to the appropriate schedule if regular updates are needed
4. Update this README with the new data source information

### Testing

Test that definitions load correctly:

```bash
cd dg_projects/data_platform
uv run python -c "from data_platform.definitions import defs; print('OK')"
```

List all definitions:

```bash
cd dg_projects/data_platform
uv run dg list defs
```

## Resources

- [OpenMetadata Documentation](https://docs.open-metadata.org/latest/)
- [OpenMetadata Python SDK](https://docs.open-metadata.org/latest/sdk/python)
- [OpenMetadata Connectors](https://docs.open-metadata.org/latest/connectors)
- [Dagster Documentation](https://docs.dagster.io/)
