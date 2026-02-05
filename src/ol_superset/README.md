# MIT Open Learning Superset Configuration

This directory contains Apache Superset asset definitions (dashboards, charts, datasets) for the MIT Open Learning data platform, managed as code using the `ol-superset` CLI.

## Quick Start

```bash
# Install CLI
cd src/ol_superset
uv sync

# Export production assets
ol-superset export

# Validate assets
ol-superset validate

# Sync to QA
ol-superset sync superset-production superset-qa

# Promote QA to production
ol-superset promote
```

## CLI Documentation

See [scripts/README.md](scripts/README.md) for complete CLI documentation, including:
- Command reference
- Typical workflows
- Authentication setup
- Troubleshooting

## Project Structure

```
ol_superset/
├── assets/              # Exported Superset assets (version controlled)
│   ├── dashboards/      # Dashboard definitions
│   ├── charts/          # Chart definitions
│   ├── datasets/        # Dataset definitions
│   └── databases/       # Database connection configs
├── ol_superset/         # CLI implementation
│   ├── cli.py          # Main entry point
│   ├── commands/       # Command implementations
│   └── lib/            # Shared utilities
├── scripts/            # Documentation
│   └── README.md       # Complete CLI guide
├── pyproject.toml      # Python package configuration
└── sync_config.yml     # Sync configuration reference
```

## Asset Management Workflow

1. **Make changes** in Superset UI (QA or Production)
2. **Export** assets with `ol-superset export --from <instance>`
3. **Deduplicate** if syncing between environments: `ol-superset dedupe`
4. **Validate** with `ol-superset validate`
5. **Review** changes with `git diff assets/`
6. **Commit** to version control
7. **Sync** to other environments as needed

### Preventing Asset Duplication

When exporting assets from multiple environments (QA and Production), you may get duplicate files for the same asset because database IDs differ between environments while UUIDs remain consistent.

**Problem**: `marts__combined__users_35.yaml` (Production ID) and `marts__combined__users_127.yaml` (QA ID) are the same asset.

**Solution**: Use UUID-based naming with the `dedupe` command:

```bash
# Preview what would change (recommended first step)
ol-superset dedupe --dry-run

# Deduplicate and rename all assets to UUID-based naming
ol-superset dedupe

# Process only specific asset types
ol-superset dedupe --datasets
ol-superset dedupe --charts --dashboards
```

**Result**: Files are renamed to use UUIDs: `marts__combined__users_5f006731-f052-4586-88f2-ad1b3c904ca9.yaml`

Run this after exporting from an environment that previously imported from another environment.

## Production Safety

The CLI includes multiple safeguards for production deployments:

- ✅ **QA → Production only** for `promote` command
- ✅ **Validation** before promoting
- ✅ **Uncommitted changes** detection
- ✅ **Explicit confirmation** required ("PROMOTE")
- ✅ **Dry-run mode** for previewing changes
- ✅ **Promotion manifest** creation

## QA Environment Management

When syncing assets to the QA environment, the CLI automatically:

- ✅ **Disables external management** for all pushed assets
- ✅ **Enables UI editing** of dashboards and charts
- ✅ **Authenticates via OAuth2** with PKCE flow
- ✅ **Updates via Superset REST API** (/api/v1/dashboard, /api/v1/chart)

This ensures that assets synced to QA remain editable in the Superset UI for testing and iteration, while production assets remain locked to prevent accidental modifications.

## Authentication

Uses the `sup` CLI for Superset API access. Configure with:

```bash
sup config auth
```

## See Also

- [WORKFLOWS.md](WORKFLOWS.MD) - Detailed workflow documentation
- [scripts/README.md](scripts/README.md) - Complete CLI reference
- [Apache Superset Documentation](https://superset.apache.org/)
