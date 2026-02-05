# Superset Asset Management CLI

Modern CLI for managing Superset assets (dashboards, charts, datasets) across environments.

## Installation

The CLI is automatically installed when you set up the ol_superset package:

```bash
cd src/ol_superset
uv sync
```

This creates the `ol-superset` command in your virtual environment.

## Quick Start

```bash
# Export production assets (backup)
ol-superset export

# Export from QA
ol-superset export --from superset-qa

# Validate assets
ol-superset validate

# Sync production to QA
ol-superset sync superset-production superset-qa

# Promote QA changes to production (with safety checks)
ol-superset promote
```

## Commands

### `ol-superset export`

Export all assets from a Superset instance.

**Options:**
- `--from`, `-f`: Instance to export from (default: `superset-production`)
- `--output-dir`, `-o`: Output directory (default: `assets/`)

**Examples:**
```bash
# Export from production (default)
ol-superset export

# Export from QA
ol-superset export --from superset-qa

# Export to custom directory
ol-superset export -f superset-qa -o /tmp/qa-backup
```

### `ol-superset validate`

Validate asset YAML files for syntax errors and security issues.

**Options:**
- `--assets-dir`, `-d`: Assets directory to validate (default: `assets/`)

**Examples:**
```bash
# Validate default assets directory
ol-superset validate

# Validate custom directory
ol-superset validate --assets-dir /tmp/qa-backup
```

### `ol-superset sync`

Sync assets from one instance to another with automatic database UUID mapping.

**Arguments:**
- `SOURCE`: Source instance name (required)
- `TARGET`: Target instance name (required)

**Options:**
- `--assets-dir`, `-d`: Assets directory (default: `assets/`)
- `--yes`, `-y`: Skip confirmation prompt
- `--dry-run`, `-n`: Preview what would be synced without syncing
- `--map-only`, `-m`: Only map database UUIDs, don't push assets to target

**Examples:**
```bash
# Sync production to QA (most common)
ol-superset sync superset-production superset-qa

# Sync with auto-confirmation (for CI/CD)
ol-superset sync superset-production superset-qa --yes

# Preview sync without making changes
ol-superset sync superset-production superset-qa --dry-run

# Only map database UUIDs without pushing (useful for manual review)
ol-superset sync superset-production superset-qa --map-only
```

**Map-Only Mode:**

The `--map-only` flag is useful when you want to:
1. Update database UUIDs in your local asset files to match the target environment
2. Review the changes before pushing
3. Manually push specific assets instead of syncing everything

```bash
# Map UUIDs to match QA environment
ol-superset sync superset-production superset-qa --map-only

# Review changes
git diff

# Manually push specific assets
sup dataset push assets/datasets/Trino/marts__combined__users_*.yaml --instance superset-qa
```

### `ol-superset promote`

Promote assets from QA to production with extensive safety checks.

**Safety Features:**
- Validates assets before promoting
- Checks for uncommitted git changes
- Requires typing "PROMOTE" to confirm
- Shows detailed summary of changes
- Creates promotion manifest

**Options:**
- `--assets-dir`, `-d`: Assets directory (default: `assets/`)
- `--skip-validation`: Skip validation (not recommended)
- `--force`, `-f`: Skip all safety checks (DANGEROUS)
- `--dry-run`, `-n`: Preview what would be promoted

**Examples:**
```bash
# Standard promotion workflow
ol-superset promote

# Preview promotion
ol-superset promote --dry-run

# Emergency deployment (use with extreme caution)
ol-superset promote --force
```

### `ol-superset dedupe`

Deduplicate and rename assets to UUID-based naming to prevent file thrashing when syncing between environments.

**Problem Solved:**
When exporting from QA (which imported from Production), you get duplicate files with different database IDs but the same UUID:
- `marts__combined__users_35.yaml` (Production database ID)
- `marts__combined__users_127.yaml` (QA database ID)

These are the SAME asset, just with different auto-increment IDs in each database.

**Solution:**
The `dedupe` command:
1. Identifies duplicate files by UUID
2. Removes duplicates (keeps first occurrence)
3. Renames files to use UUID instead of database ID: `marts__combined__users_5f006731-f052-4586-88f2-ad1b3c904ca9.yaml`

**Options:**
- `--assets-dir`: Assets directory (default: `assets/`)
- `--datasets`: Only process datasets
- `--charts`: Only process charts
- `--dashboards`: Only process dashboards
- `--dry-run`: Preview changes without making them

**Examples:**
```bash
# Preview what would be done (recommended first step)
ol-superset dedupe --dry-run

# Deduplicate and rename all assets
ol-superset dedupe

# Only process datasets
ol-superset dedupe --datasets

# Process charts and dashboards but not datasets
ol-superset dedupe --charts --dashboards
```

**When to use:**
- After exporting from an environment that imported assets from another environment
- Before committing assets to avoid duplicate files in git
- As part of your regular sync workflow between environments

## Typical Workflows

### Weekly Production Backup

```bash
cd /path/to/ol-data-platform/src/ol_superset

# Export production assets
ol-superset export

# Review changes
git diff assets/

# Commit backup
git add assets/
git commit -m "Weekly Superset production backup"
git push
```

### Sync Production to QA

```bash
# Export from production
ol-superset export --from superset-production

# Sync to QA
ol-superset sync superset-production superset-qa

# Verify in QA
open https://bi-qa.ol.mit.edu/dashboard/list/
```

### Test Changes in QA, Then Promote to Production

```bash
# 1. Make changes in QA Superset UI
#    https://bi-qa.ol.mit.edu

# 2. Export from QA
ol-superset export --from superset-qa

# 3. Deduplicate if needed (QA imported from production)
ol-superset dedupe --dry-run  # Preview first
ol-superset dedupe            # Apply changes

# 4. Review changes
git diff assets/

# 5. Validate
ol-superset validate

# 6. Commit changes
git add assets/
git commit -m "Add new enrollment dashboard"
git push

# 7. After review, promote to production
ol-superset promote
```

### Handling Asset Duplication Between Environments

If you're syncing assets between QA and Production, you may encounter duplicate files:

```bash
# Problem: After exporting from QA, you see duplicates:
#   assets/datasets/Trino/marts__combined__users_35.yaml   (Production ID)
#   assets/datasets/Trino/marts__combined__users_127.yaml  (QA ID)

# Solution: Use dedupe to consolidate to UUID-based naming
ol-superset dedupe --dry-run  # Preview changes
ol-superset dedupe            # Apply changes

# Result: Single file with UUID:
#   assets/datasets/Trino/marts__combined__users_5f006731-f052-4586-88f2-ad1b3c904ca9.yaml

# Commit the consolidated assets
git add assets/
git commit -m "chore: Deduplicate assets to UUID-based naming"
```

## Authentication

The CLI uses the `sup` CLI which requires configured instances:

```bash
# List configured instances
sup instance list

# Configure a new instance
sup config auth

# Use specific instance
sup instance use superset-production
```

See `~/.sup/config.yml` for instance configurations.

## Production Safety

The `promote` command has multiple safeguards:

1. **QA → Production Only**: Hardcoded to only allow this direction
2. **Validation**: Automatically validates assets before promoting
3. **Git Check**: Warns about uncommitted changes
4. **Confirmation**: Requires typing "PROMOTE" (exact, case-sensitive)
5. **Manifest**: Creates a record of what was promoted

The `sync` command also protects production:

1. **Warning**: Shows clear warning when targeting production
2. **Confirmation**: Requires typing "SYNC TO PRODUCTION" for production targets
3. **Dry Run**: Always available to preview changes

## Troubleshooting

### Command not found: ol-superset

```bash
# Make sure you're in the virtual environment
cd src/ol_superset
source .venv/bin/activate

# Or use uv to run
uv run ol-superset --help
```

### Command not found: sup

```bash
# Install sup CLI
cd ~/src/superset-sup
uv tool install --force --reinstall .
```

### Authentication failed

```bash
# Reconfigure instance
sup config auth
# Select self-hosted Superset and follow prompts
```

## Development

The CLI is structured as:

```
ol_superset/
├── cli.py              # Main entry point
├── commands/           # Command implementations
│   ├── export.py
│   ├── sync.py
│   ├── promote.py
│   └── validate.py
└── lib/                # Shared utilities
    ├── utils.py
    └── database_mapping.py
```

To add new commands, create a function in `commands/` and register it in `cli.py`.

## See Also

- [WORKFLOWS.md](../WORKFLOWS.md) - Detailed workflow documentation
- [cyclopts documentation](https://cyclopts.readthedocs.io/) - CLI framework docs
- `ol-superset --help` - Command help
- `ol-superset <command> --help` - Command-specific help
