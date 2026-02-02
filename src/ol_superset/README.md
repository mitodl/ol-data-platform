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
3. **Validate** with `ol-superset validate`
4. **Review** changes with `git diff assets/`
5. **Commit** to version control
6. **Sync** to other environments as needed

## Production Safety

The CLI includes multiple safeguards for production deployments:

- ✅ **QA → Production only** for `promote` command
- ✅ **Validation** before promoting
- ✅ **Uncommitted changes** detection
- ✅ **Explicit confirmation** required ("PROMOTE")
- ✅ **Dry-run mode** for previewing changes
- ✅ **Promotion manifest** creation

## Authentication

Uses the `sup` CLI for Superset API access. Configure with:

```bash
sup config auth
```

## See Also

- [WORKFLOWS.md](WORKFLOWS.MD) - Detailed workflow documentation
- [scripts/README.md](scripts/README.md) - Complete CLI reference
- [Apache Superset Documentation](https://superset.apache.org/)
