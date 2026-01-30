# Lakehouse Local Development Guide

## Quick Start

### Speed Up Local Development

By default, the lakehouse code location connects to Airbyte to discover and load assets. This can slow down local development when you're working on non-Airbyte changes (e.g., dbt models, dlt pipelines).

To skip Airbyte asset loading for faster iteration:

```bash
export SKIP_AIRBYTE=1

# Now commands run much faster
dg list defs
dagster dev
```

**When to use SKIP_AIRBYTE:**
- Working on dbt models
- Working on dlt pipelines
- Testing Dagster configuration changes
- Iterating on non-Airbyte assets

**When NOT to use SKIP_AIRBYTE:**
- Making changes to Airbyte connections
- Testing Airbyte asset jobs
- Need to see complete asset graph with Airbyte dependencies

### Running dlt Pipelines Locally

dlt pipelines can run standalone without Dagster:

```bash
# Run with local destination (writes to .dlt/data/)
python -m lakehouse.defs.qualtrics_ingestion.loads

# Query results with DuckDB
duckdb -c "
INSTALL iceberg;
LOAD iceberg;
SELECT * FROM read_parquet('.dlt/data/surveys/*.parquet') LIMIT 10;
"
```

See [dlt Integration Guide](./dlt_integration_guide.md) for more details.

## Environment Variables

### SKIP_AIRBYTE
- **Values**: `1`, `true`, `yes` (case-insensitive) to skip; anything else loads normally
- **Effect**: Disables Airbyte connection and asset loading
- **Use case**: Speed up local development when not working on Airbyte

### DLT_DESTINATION_ENV
- **Values**: `local` (default) or `production`
- **Effect**: Controls where dlt pipelines write data
  - `local`: Writes to `.dlt/data/` on local disk
  - `production`: Writes to S3 bucket configured in `.dlt/config.toml`
- **Use case**: Test dlt pipelines locally before deploying

### DAGSTER_ENV
- **Values**: `dev`, `ci`, `qa`, `production`
- **Effect**: Controls Dagster environment configuration
- **Default**: `dev` for local development

## Common Workflows

### Working on dbt Models

```bash
export SKIP_AIRBYTE=1
cd /path/to/ol-data-platform/dg_projects/lakehouse

# Start Dagster UI
dagster dev

# Your dbt models will be available without waiting for Airbyte
```

### Working on dlt Sources

```bash
# Run pipeline locally
python -m lakehouse.defs.qualtrics_ingestion.loads

# Iterate on source configuration in loads.py
# Re-run to test changes

# When ready, test in Dagster
export SKIP_AIRBYTE=1  # Optional: skip Airbyte for faster loading
dg list defs | grep qualtrics
```

### Testing Airbyte Changes

```bash
# DON'T set SKIP_AIRBYTE
unset SKIP_AIRBYTE

# Now Airbyte assets will load
dg list defs | grep airbyte
dagster dev
```

## Helper Scripts

### Check dlt Environment

```bash
python scripts/dlt_env.py status
```

### Switch dlt Destination

```bash
# Local development
python scripts/dlt_env.py local

# Production testing
python scripts/dlt_env.py production
```

## Troubleshooting

### Slow Dagster Loading

**Problem**: `dg list defs` or `dagster dev` takes a long time to start

**Solution**: Use `SKIP_AIRBYTE=1` if you're not working on Airbyte assets

```bash
export SKIP_AIRBYTE=1
dg list defs
```

### dlt Configuration Warnings

**Problem**: Seeing warnings about missing configuration or placeholder values

**Solution**: Configure credentials in `.dlt/secrets.toml` (see template at `.dlt/secrets.toml.template`)

### Missing Assets in Dagster

**Problem**: Can't see Airbyte assets in Dagster UI

**Solution**: Make sure `SKIP_AIRBYTE` is not set

```bash
unset SKIP_AIRBYTE
dagster dev
```

## References

- [dlt Integration Guide](./dlt_integration_guide.md) - Complete guide to using dlt in lakehouse
- [Qualtrics Example](../lakehouse/defs/qualtrics_ingestion/README.md) - Working example of a dlt pipeline
