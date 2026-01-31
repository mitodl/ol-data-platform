# Lakehouse Documentation

This directory contains documentation for the lakehouse Dagster code location.

## Guides

### [Local Development Guide](./local_development.md)
Quick start guide for local development including:
- Skipping Airbyte for faster iteration (`SKIP_AIRBYTE`)
- Running dlt pipelines locally
- Common workflows and troubleshooting

### [dlt Integration Guide](./dlt_integration_guide.md)
Comprehensive guide to using dlt for data ingestion:
- When to use dlt vs Airbyte
- Creating new dlt sources
- LLM-native development workflow
- Dagster integration patterns
- Configuration and secrets management

## Quick Links

### Getting Started
- **Skip Airbyte** for faster development: `export SKIP_AIRBYTE=1`
- **Run dlt pipeline** locally: `python -m lakehouse.defs.qualtrics_ingestion.loads`
- **Check dlt environment**: `python scripts/dlt_env.py status`

### Examples
- [Qualtrics dlt Pipeline](../lakehouse/defs/qualtrics_ingestion/README.md) - Complete working example

### Configuration Files
- `.dlt/config.toml` - dlt non-sensitive configuration
- `.dlt/secrets.toml.template` - Template for credentials (copy to `secrets.toml`)
- `pyproject.toml` - Python dependencies

## Architecture

The lakehouse code location includes:

- **Airbyte Assets**: Ingestion from external sources via Airbyte (can be skipped with `SKIP_AIRBYTE=1`)
- **dlt Assets**: Python-based data ingestion via dlt (new sources should use this)
- **dbt Assets**: Transformations and modeling via dbt
- **Superset Assets**: Dataset synchronization with Superset

## Contributing

### Adding a New Data Source

**Use dlt** for new REST APIs and file-based sources:

1. Initialize source: `dlt init dlthub:source_name filesystem`
2. Create defs structure: `lakehouse/defs/source_ingestion/`
3. Adapt code to `loads.py` and create `defs.yaml`
4. Test locally, then deploy to Dagster

See the [dlt Integration Guide](./dlt_integration_guide.md) for details.

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

2. **Test changes** in your specific area (dlt, dbt, etc.)

3. **Validate** in Dagster UI or with `dg` CLI

4. **Deploy** via your normal deployment process

## Support

For questions or issues:
- Check [Troubleshooting](./local_development.md#troubleshooting) section
- Review [dlt Integration Guide](./dlt_integration_guide.md)
- See working [Qualtrics Example](../lakehouse/defs/qualtrics_ingestion/README.md)
