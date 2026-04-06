## dbt

This project is configured to use Trino as the warehouse engine (QA and Production)
and DuckDB for local development. Profile information is defined in `profiles.yml`
with environment variables for credentials. The default target is `dev_local` (DuckDB).

### Models Structure

dbt models (SQL files) are structured into layers under `ol_dbt/models/`, organised by
product within each layer (e.g. `mitxonline`, `mitxpro`).

- **Staging** — 1:1 with source tables; clean, rename, cast. Naming: `stg_<source>__<table>.sql`
- **Intermediate** — complex joins and aggregations. Naming: `int_<domain>__<description>.sql`
- **Dimensional** — star-schema dimensional warehouse (`dim_*`, `tfact_*`, `afact_*`,
  `bridge_*`). Located in `models/dimensional/`. This layer is the **architectural
  boundary** between data preparation and data consumption. See
  [Dimensional Model Conventions](models/dimensional/README.md) for conventions.
- **Marts** — analytics-ready, customer-facing tables that build exclusively on the
  dimensional layer. Naming: `marts__<domain>__<description>.sql`.
- **Reporting** — report-shaped views for BI tools and external consumers, built
  exclusively on dimensional and/or mart models.

> **Convention:** Marts and reporting models must only reference models in the
> `dimensional/` layer (or other mart/reporting models that themselves follow this rule).
> Direct references to `staging/` or `intermediate/` models from marts or reports are
> architectural violations and should be migrated to use the appropriate dimensional model.
> If a required dimensional model does not yet exist, it should be added to the dimensional
> layer before the mart/report is updated.

### Local Setup

```bash
# Clone the repo and enter the project
git clone git@github.com:mitodl/ol-data-platform.git
cd ol-data-platform

# Install all dependencies (includes dbt, ol-dbt CLI, and adapters)
uv sync

# Install dbt packages (run from src/ol_dbt so dbt finds profiles.yml)
cd src/ol_dbt && dbt deps
```

> **Note:** When running `dbt` commands directly, your working directory must be
> `src/ol_dbt/` (where `profiles.yml` lives), or set `DBT_PROFILES_DIR=src/ol_dbt`
> before running from the repo root. The `ol-dbt run` command handles this automatically.

For Trino targets (`dev_qa`, `dev_production`) you also need:
```bash
export DBT_TRINO_USERNAME=<USERNAME>/accountadmin
export DBT_TRINO_PASSWORD=<PASSWORD>
```
Starburst credentials can be requested from OL DevOps.

#### Available targets

| Target | Engine | Use case |
|--------|--------|----------|
| `dev_local` *(default)* | DuckDB (local file) | Fast local iteration, no credentials needed |
| `dev_qa` | Trino — QA cluster (OAuth) | Integration testing against real QA data |
| `dev_production` | Trino — Production cluster (OAuth) | Read from production data |
| `qa` | Trino — QA cluster (LDAP) | CI / automated jobs |
| `production` | Trino — Production cluster (LDAP) | CI / automated jobs |

### Iterative Development with `ol-dbt run` (Recommended)

The `ol-dbt run` command wraps `dbt build` with **automatic state-based incremental
execution** so you only rebuild what has changed or previously errored — not the entire
project on every iteration.

```bash
# First run: full build, saves state to src/ol_dbt/.dbt-state/
ol-dbt run

# Subsequent runs: only changed models + previously failed nodes
ol-dbt run

# Force a complete rebuild (and refresh state for the next run)
ol-dbt run --full-refresh

# Run against the QA Trino cluster instead of local DuckDB
ol-dbt run --target dev_qa

# Explicit model selection (state-based defer still active for upstream refs)
ol-dbt run --select my_model+

# Run models only (skip tests)
ol-dbt run run

# Run tests only
ol-dbt run test
```

**How it works:**

1. On the **first run** (no `.dbt-state/` directory) the full project is built and
   `manifest.json` + `run_results.json` are saved to `src/ol_dbt/.dbt-state/`.
2. On **subsequent runs** dbt uses `--select "state:modified+ result:error+ result:fail+"`
   to target only models whose content hash changed since the last run plus any nodes
   that errored or failed previously.
3. `--defer` is enabled by default so upstream `ref()` calls for un-built parents
   resolve against the saved state manifest rather than requiring a full local build.

The `.dbt-state/` directory is gitignored and local to your machine.

### Running dbt Directly

You can still invoke dbt commands directly from `src/ol_dbt/`:

```bash
# Compile all models to verify SQL (fast, no warehouse needed)
dbt compile

# Run a specific model
dbt run --select int__mitxonline__users

# Run a directory of models
dbt run --select staging.mitxonline

# Run and test a model (equivalent to dbt build for one model)
dbt build --select int__mitxonline__users

# Force-recreate a model
dbt run --select int__mitxonline__users --full-refresh

# Run all tests for a model
dbt test --select int__mitxonline__users

# Run against QA Trino
dbt run --select my_model --target dev_qa --vars '{"schema_suffix": "myname"}'
```

> **Note**: `--vars 'schema_suffix: <value>'` namespaces your dev schemas on Trino
> targets so they don't collide with other developers' schemas.

### Other dbt Commands

```bash
# Build and test all resources (models + tests + snapshots + seeds)
dbt build

# Delete compiled artifacts and installed packages
dbt clean

# Generate and serve project documentation locally
dbt docs generate
dbt docs serve  # default port 8080
```

See the [dbt command reference](https://docs.getdbt.com/reference/dbt-commands) for all available commands.

### Resources

- [dbt documentation](https://docs.getdbt.com/docs/introduction)
- [dbt Discourse](https://discourse.getdbt.com/) — commonly asked questions
- [dbt Community Slack](https://community.getdbt.com/)
