# Lakehouse Documentation

This directory contains documentation for the lakehouse Dagster code location.

## Guides

### [Local Development Guide](./local_development.md)
Quick start guide for local development including:
- Skipping Airbyte for faster iteration (`SKIP_AIRBYTE`)
- Common workflows and troubleshooting

## Quick Links

### Getting Started
- **Skip Airbyte** for faster development: `export SKIP_AIRBYTE=1`

## Architecture

The lakehouse code location includes:

- **Airbyte Assets**: Ingestion from external sources via Airbyte (can be skipped with `SKIP_AIRBYTE=1`)
- **dbt Assets**: Transformations and modeling via dbt
- **Superset Assets**: Dataset synchronization with Superset

## Contributing

### Adding a New Data Source

**Use Airbyte** for:
- Sources with existing maintained Airbyte connectors
- Database CDC
- Standard databases

### Development Workflow

1. **Local development** with fast iteration:
   ```bash
   export SKIP_AIRBYTE=1  # Optional: skip Airbyte
   dagster dev
   ```

2. **Test changes** in your specific area (dbt, etc.)

3. **Validate** in Dagster UI or with `dg` CLI

4. **Deploy** via your normal deployment process

## Support

For questions or issues:
- Check [Troubleshooting](./local_development.md#troubleshooting) section
