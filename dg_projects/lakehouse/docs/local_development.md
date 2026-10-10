# Lakehouse Local Development Guide

## Quick Start

### Speed Up Local Development

By default, the lakehouse code location connects to Airbyte to discover and load assets. This can slow down local development when you're working on non-Airbyte changes (e.g., dbt models).

To skip Airbyte asset loading for faster iteration:

```bash
export SKIP_AIRBYTE=1

# Now commands run much faster
dg list defs
dagster dev
```

**When to use SKIP_AIRBYTE:**
- Working on dbt models
- Testing Dagster configuration changes
- Iterating on non-Airbyte assets

**When NOT to use SKIP_AIRBYTE:**
- Making changes to Airbyte connections
- Testing Airbyte asset jobs
- Need to see complete asset graph with Airbyte dependencies

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

## StarRocks in `dev`

With `DAGSTER_ENVIRONMENT` unset or `dev`, the StarRocks assets
(`starrocks_dbt_assets` and the materialized view refresh) connect to
the StarRocks in ol-infrastructure's local-dev cluster. That is the k3d stack
with `data-platform` in `enabled_apps`, which Tilt forwards to `127.0.0.1:9030`.
They log in as its passwordless `root` and need no Vault token.

The code location still attempts a Vault login when it loads, for its other
resources, and without a cached token that opens the OIDC browser flow. Set
`VAULT_OIDC_NONINTERACTIVE=1` to skip it: the login fails, the location loads
with a warning, and the StarRocks resource connects to the local cluster as
before.

- dbt target: `starrocks_local_b2b`. The b2b materialized views are built in
  `default_catalog.b2b_analytics` and `default_catalog.b2b_learner_records`.
- Lake: the views read `ol_data_lake_local.ol_warehouse_local_dimensional`.
  Nothing fills that schema yet, so a build fails on a missing table until the
  tables a view reads exist.
- `ol-dbt starrocks build --env dev --target starrocks_local_b2b --select tag:starrocks`
  runs the same build without Dagster.

To use the QA cluster instead, run with `DAGSTER_ENVIRONMENT=qa`, which is what
`docker-compose.yaml` sets for this code location. That needs a Vault login
(`bin/vault-login`).

`DAGSTER_DBT_STARROCKS_TARGET` cannot cross that line. The host follows the
environment and the login follows the target, so a Vault-backed target under
`dev`, or a local target under any other environment, fails when the code
location loads.

The Trino side of `dev` is unchanged and still reads production.

## Common Workflows

### Working on dbt Models

```bash
export SKIP_AIRBYTE=1
cd /path/to/ol-data-platform/dg_projects/lakehouse

# Start Dagster UI
dagster dev

# Your dbt models will be available without waiting for Airbyte
```

### Testing Airbyte Changes

```bash
# DON'T set SKIP_AIRBYTE
unset SKIP_AIRBYTE

# Now Airbyte assets will load
dg list defs | grep airbyte
dagster dev
```

## Troubleshooting

### Slow Dagster Loading

**Problem**: `dg list defs` or `dagster dev` takes a long time to start

**Solution**: Use `SKIP_AIRBYTE=1` if you're not working on Airbyte assets

```bash
export SKIP_AIRBYTE=1
dg list defs
```

**Solution**: Configure dlt credentials in the data loading project, using `dg_projects/data_loading/.dlt/secrets.toml` created from the template at `dg_projects/data_loading/.dlt/secrets.toml.template`

### Missing Assets in Dagster

**Problem**: Can't see Airbyte assets in Dagster UI

**Solution**: Make sure `SKIP_AIRBYTE` is not set

```bash
unset SKIP_AIRBYTE
dagster dev
```
