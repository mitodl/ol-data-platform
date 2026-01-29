# Superset Asset Management

This directory contains version-controlled Superset assets (dashboards, charts, datasets, databases) that can be synchronized between QA and Production environments.

📖 **See [WORKFLOWS.md](./WORKFLOWS.md) for detailed workflow diagrams and comparisons**

## Supported Workflows

### 🔄 Primary Workflow: QA → Production (Recommended)
Develop and test in QA, then promote to production:

1. **Edit** dashboards in QA Superset UI
2. **Export** from QA → version control
3. **Review & Commit** changes
4. **Promote** to production

### 🔄 Alternative Workflow: Production → QA
Mirror production assets to QA for testing:

1. **Export** from production → version control
2. **Review & Commit** changes
3. **Deploy** to QA for testing

## Directory Structure

```
src/ol_superset/
├── README.md                 # This file
├── sync_config.yml           # Configuration for syncing to QA
├── assets/                   # All Superset assets (auto-generated)
│   ├── dashboards/           # Dashboard YAML definitions
│   ├── charts/               # Chart YAML definitions
│   ├── datasets/             # Dataset YAML definitions
│   └── databases/            # Database connection configs
└── scripts/                  # Automation scripts
    ├── export_all.sh         # Export from production
    ├── export_from_qa.sh     # Export from QA (for promotion)
    ├── promote_to_production.sh  # Promote QA → Production
    ├── sync_to_qa.sh         # Deploy Production → QA
    └── validate_assets.sh    # Validate asset definitions
```

## Prerequisites

1. **Install sup CLI**: Already available at `/home/tmacey/.local/bin/sup`
2. **Configure authentication**: `sup config` (already configured for production/QA)
3. **Set active instance**: `sup instance use superset-production`

## Common Workflows

### Workflow 1: QA → Production (Primary/Recommended)

**Use case**: Developing new dashboards or modifying existing ones

```bash
cd src/ol_superset

# 1. Make changes in QA Superset UI
#    https://bi-qa.ol.mit.edu

# 2. Export latest from QA
./scripts/export_from_qa.sh

# 3. Validate changes
./scripts/validate_assets.sh

# 4. Review what changed
git diff assets/

# 5. Commit changes with descriptive message
git add assets/
git commit -m "Add new enrollment dashboard with demographic filters"
git push

# 6. Promote to production (automated with sup dashboard push)
./scripts/promote_to_production.sh

# 7. Verify in production UI
#    https://bi.ol.mit.edu/dashboard/list/
```

### Workflow 2: Production → QA (Mirroring)

**Use case**: Copying production dashboards to QA for testing changes

```bash
cd src/ol_superset

# 1. Export from production
./scripts/export_all.sh

# 2. Review and commit
git diff assets/
git add assets/ && git commit -m "Mirror production dashboards"

# 3. Deploy to QA (automated with sup dashboard push)
./scripts/sync_to_qa.sh
```

### Workflow 3: Regular Backups

**Use case**: Periodic snapshots of production for disaster recovery

```bash
cd src/ol_superset

# Export and commit (weekly via cron or GitHub Actions)
./scripts/export_all.sh
git add assets/
git commit -m "Backup: Production assets - $(date +%Y-%m-%d)"
git push
```

## Common Workflows (Detailed Steps)

Export all dashboards, charts, datasets, and databases:

```bash
cd src/ol_superset
./scripts/export_all.sh
```

This pulls all published dashboards along with their dependencies (charts, datasets, databases).

### 2. Export Specific Dashboard

To export a single dashboard with dependencies:

```bash
cd src/ol_superset
sup dashboard pull assets/ --id=52
```

### 3. Sync Assets to QA (Dry Run)

Preview what changes would be applied to QA:

```bash
cd src/ol_superset
sup sync run . --dry-run
```

### 4. Deploy to QA

### Deploy to QA (from production)

Execute the sync to push production assets to QA for testing:

```bash
cd src/ol_superset
./scripts/sync_to_qa.sh
```

### Validate Assets

Check that all asset files are valid:

```bash
cd src/ol_superset
./scripts/validate_assets.sh
```

## Working with Assets

### Pull Commands

```bash
# Pull all dashboards + dependencies (charts, datasets, databases)
sup dashboard pull assets/

# Pull only published dashboards
sup dashboard pull assets/ --search="status:published"

# Pull specific dashboard by ID
sup dashboard pull assets/ --id=52

# Pull multiple dashboards
sup dashboard pull assets/ --ids=52,58,14

# Pull only dashboards you own
sup dashboard pull assets/ --mine

# Pull without dependencies (dashboards only)
sup dashboard pull assets/ --skip-dependencies
```

### Push Commands

```bash
# Push charts to QA or production instance
sup instance use superset-qa
sup chart push assets/ --overwrite --force

# Push with automatic database UUID mapping (recommended for cross-environment)
sup chart push assets/ --auto-map-databases --overwrite --force

# Push dashboards (includes charts, datasets, databases as dependencies)
sup instance use superset-production
sup dashboard push assets/ --overwrite --force

# Push with automatic database mapping by name
sup dashboard push assets/ --auto-map-databases --overwrite --force

# Push to specific instance without changing context
sup dashboard push assets/ --instance superset-qa --overwrite

# Push with specific database UUID (all assets use this database)
sup dashboard push assets/ --database-uuid "abc-123-def" --overwrite

# Push with database name lookup (fetches UUID from target)
sup dashboard push assets/ --database-name "Trino" --overwrite

# Push with template variables
sup dashboard push assets/ --option ENV=production --option REGION=us-east

# Continue on error (useful for batch imports)
sup dashboard push assets/ --continue-on-error --force
```

#### Database UUID Transformation

When importing assets from one environment to another (e.g., production → QA), database UUIDs often don't match. Use these options to automatically transform database references:

- `--auto-map-databases`: **Recommended**. Automatically matches databases by name between source and target. Best for environments with similarly named databases.
- `--database-name "Name"`: Use a specific database from the target by name. All assets will reference this database.
- `--database-uuid "uuid"`: Use a specific UUID. All assets will reference this database UUID.

**Example**: Syncing production assets to QA with auto-mapped databases:
```bash
sup instance use superset-qa
sup dashboard push assets/ --auto-map-databases --overwrite --force
```

**Note**: Both `chart push` and `dashboard push` automatically import all dependencies (datasets, databases) so you don't need to push them separately. Database transformation is applied before import to ensure compatibility with the target environment.

## Sync Configuration

The `sync_config.yml` file defines:
- **Source**: superset-production instance
- **Targets**: superset-qa instance (can add more)
- **Variables**: Environment-specific values (database connections, schema names)
- **Filters**: Which assets to sync

### Jinja2 Templating

Assets support Jinja2 variables for environment-specific customization:

```yaml
# In asset files
database_name: "{{ database }}"
schema: "ol_warehouse_{{ environment }}_mart"
```

Variables are defined in `sync_config.yml`:

```yaml
variables:
  production:
    environment: "production"
    database: "Trino Production"
  qa:
    environment: "qa"
    database: "Trino QA"
```

## Security Notes

⚠️ **Never commit secrets to version control**

- Database passwords are excluded from exports
- API tokens should be managed via environment variables or Vault
- The `.gitignore` prevents accidental secret commits
- Use Jinja2 variables for environment-specific credentials

## Maintenance

### Regular Export Cadence

Establish a regular schedule for exporting production assets:

```bash
# Weekly export (suggested cron job or GitHub Action)
cd /home/tmacey/code/mit/data/ol-data-platform/src/ol_superset
./scripts/export_all.sh
git add assets/
git commit -m "Update Superset assets from production - $(date +%Y-%m-%d)"
git push
```

### After Making Changes in Superset UI

**For QA → Production flow** (recommended):

1. Export from QA: `./scripts/export_from_qa.sh`
2. Review changes: `git diff assets/`
3. Commit: `git add assets/ && git commit -m "Description of changes"`
4. Promote: `./scripts/promote_to_production.sh`

**For Production → QA flow** (mirroring):

1. Export from production: `./scripts/export_all.sh`
2. Review changes: `git diff assets/`
3. Commit: `git add -p && git commit -m "Update from production"`
4. Deploy to QA: `./scripts/sync_to_qa.sh`

## Troubleshooting

### Authentication Issues

```bash
# Re-authenticate
sup config

# Verify current instance
sup instance list
```

### Asset Conflicts

If sync encounters conflicts:
1. Use `--dry-run` to preview changes
2. Use `--overwrite` flag to force updates
3. Review diffs before applying

### Dependencies Not Pulled

Ensure you're not using `--skip-dependencies` flag when pulling dashboards.

## References

- [sup CLI Documentation](https://github.com/preset-io/backend-sdk)
- [Superset Documentation](https://superset.apache.org/)
- Internal: `dg_projects/lakehouse/lakehouse/assets/superset.py` - Dagster asset integration
