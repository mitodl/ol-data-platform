# Superset Asset Management Scripts

Scripts for managing Superset assets (dashboards, charts, datasets) across environments.

## Quick Reference

```bash
# Export all assets from production
./export_all.sh

# Export from QA
./export_all.sh superset-qa

# Sync production to QA
./export_all.sh superset-production
./sync_assets.sh superset-production superset-qa

# Sync QA to production (after testing changes)
./export_all.sh superset-qa
./sync_assets.sh superset-qa superset-production
```

## Scripts

### `export_all.sh [instance] [output-dir]`

Exports all Superset assets from specified instance.

**Arguments:**
- `instance` (optional): Instance name (default: `superset-production`)
- `output-dir` (optional): Output directory (default: `../assets`)

**What it does:**
- Fetches ALL datasets (with pagination)
- Fetches ALL charts (with pagination)
- Fetches ALL dashboards
- Exports database configurations
- Shows summary of exported assets

**Examples:**
```bash
# Export from production (default)
./export_all.sh

# Export from QA
./export_all.sh superset-qa

# Export to custom directory
./export_all.sh superset-qa /tmp/qa-backup
```

### `sync_assets.sh <source-instance> <target-instance>`

Syncs assets from one instance to another with automatic database UUID mapping.

**Arguments:**
- `source-instance`: Source instance name (e.g., `superset-production`)
- `target-instance`: Target instance name (e.g., `superset-qa`)

**What it does:**
- Maps database UUIDs automatically
- Pushes datasets to target
- Pushes charts to target
- Pushes dashboards to target
- Continues on error (resilient)
- Shows summary of synced assets

**Examples:**
```bash
# Sync production to QA (most common)
./sync_assets.sh superset-production superset-qa

# Sync QA to production
./sync_assets.sh superset-qa superset-production
```

### `map_database_uuids.py <target-instance>`

Maps database UUIDs from source assets to target instance.

**Called automatically by `sync_assets.sh`** - you typically don't need to run this directly.

**What it does:**
- Fetches database list from target instance
- Matches databases by name
- Rewrites all `database_uuid` references
- Updates database config files

**Example:**
```bash
python3 map_database_uuids.py superset-qa
```

## Authentication

Scripts use the `sup` CLI which requires configured instances:

```bash
# List configured instances
sup instance list

# Configure a new instance
sup config auth

# Use specific instance
sup instance use superset-production
```

See `~/.sup/config.yml` for instance configurations.

## Typical Workflows

### Weekly Production Backup

```bash
cd /path/to/ol-data-platform/src/ol_superset

# Export production assets
./scripts/export_all.sh superset-production

# Review changes
git diff assets/

# Commit backup
git add assets/
git commit -m "Weekly Superset production backup"
git push
```

### Sync Production to QA

```bash
cd /path/to/ol-data-platform/src/ol_superset

# Export from production
./scripts/export_all.sh superset-production

# Sync to QA (with confirmation prompt)
./scripts/sync_assets.sh superset-production superset-qa

# Verify in QA
open https://bi-qa.ol.mit.edu/dashboard/list/
```

### Test Changes in QA, Then Promote

```bash
# 1. Make changes in QA Superset UI
#    https://bi-qa.ol.mit.edu

# 2. Export from QA
./scripts/export_all.sh superset-qa

# 3. Review changes
git diff assets/

# 4. Commit changes
git add assets/
git commit -m "Add new enrollment dashboard"
git push

# 5. After review, promote to production
./scripts/sync_assets.sh superset-qa superset-production
```

## Troubleshooting

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

### Only 50 assets exported
The default page size is 50, but scripts use `--limit 1000` to fetch all assets via pagination.

### "Could not extract database information"
The script has a regex fallback that handles malformed JSON from the CLI automatically.

### Import shows errors but completes
This is expected - the `--continue-on-error` flag allows the import to skip problematic assets and continue with the rest.

## Technical Details

- **Authentication**: OAuth2 with PKCE flow
- **CSRF Protection**: Tokens automatically fetched for POST requests
- **Pagination**: Fetches all assets in 100-item pages
- **UUID Mapping**: Automatic database UUID translation between environments
- **Error Handling**: Continue-on-error for resilient imports
- **Dependencies**: Database configs included automatically

## See Also

- [WORKFLOWS.md](../WORKFLOWS.md) - Detailed workflow documentation
- `sup --help` - CLI help
- `sup <command> --help` - Command-specific help
