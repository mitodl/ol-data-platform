# Superset Asset Management Workflows

This document describes the workflows for managing Superset assets between environments (Production, QA) using the `ol-superset` CLI.

## Overview

We use a unified `ol-superset` CLI built on the `sup` tool for automated asset export/import with OAuth authentication and database UUID mapping. The workflow supports bidirectional sync between environments with proper transformation.

## Quick Start

### Export Assets from Any Environment

```bash
# Export from production (default)
ol-superset export

# Export from QA
ol-superset export --from superset-qa

# Export to custom directory
ol-superset export --from superset-qa --output-dir /tmp/qa-backup
```

### Validate Assets

```bash
# Validate assets in default directory
ol-superset validate

# Validate custom directory
ol-superset validate --assets-dir /tmp/qa-backup
```

### Sync Assets Between Environments

```bash
# Production → QA (most common)
ol-superset sync superset-production superset-qa

# QA → Production (after testing changes)
ol-superset sync superset-qa superset-production

# Preview changes with dry-run
ol-superset sync superset-production superset-qa --dry-run
```

### Promote QA to Production (Recommended)

```bash
# Promote with all safety checks
ol-superset promote

# Preview what would be promoted
ol-superset promote --dry-run
```

## Workflow Diagrams

### Primary Workflow: QA → Production (Recommended)

```
┌─────────────────────────────────────────────────────────────────────────┐
│              QA → Production Promotion Flow (Recommended)               │
└─────────────────────────────────────────────────────────────────────────┘

1. Development Phase (QA)
   ┌──────────────────┐
   │   QA Superset    │  Developer creates/edits dashboards
   │ bi-qa.ol.mit.edu │  in QA environment for testing
   └────────┬─────────┘
            │
            ▼
   ┌──────────────────┐
   │  Test & Iterate  │  Validate with QA data
   └────────┬─────────┘
            │
            ▼

2. Export & Validate Phase
   ┌──────────────────────────┐
   │ ol-superset export       │  Export from QA
   │   --from superset-qa     │
   └────────┬─────────────────┘
            │
            ▼
   ┌──────────────────────────┐
   │ ol-superset validate     │  Check YAML syntax
   │                          │  Security validation
   └────────┬─────────────────┘
            │
            ▼
   ┌──────────────────┐
   │  assets/ folder  │  YAML files on disk
   │  (Git tracked)   │  - datasets/
   └────────┬─────────┘  - charts/
            │           - dashboards/
            ▼           - databases/

3. Review & Commit Phase
   ┌──────────────────┐
   │  git diff        │  Review changes
   └────────┬─────────┘
            │
            ▼
   ┌──────────────────┐
   │  git add         │  Stage changes
   │  git commit      │  Commit with description
   │  git push        │  Push to remote
   └────────┬─────────┘
            │
            ▼

4. Promotion Phase (Safety Checks)
   ┌──────────────────────────┐
   │ ol-superset promote      │  Automated promotion with:
   │                          │  ✓ Pre-flight validation
   └────────┬─────────────────┘  ✓ Git status check
            │                    ✓ Requires "PROMOTE" confirmation
            ▼                    ✓ Creates manifest
   ┌──────────────────────────┐
   │ Production Superset      │  Assets imported automatically
   │ bi.ol.mit.edu            │  via sup CLI with DB UUID mapping
   └──────────────────────────┘

Benefits:
  ✓ Changes tested before production
  ✓ Validation before promotion
  ✓ Code review via git diff
  ✓ Audit trail in version control
  ✓ Multiple production safety checks
  ✓ Promotion manifest created
```

### Alternative Workflow: Production → QA (Mirroring)

```
┌─────────────────────────────────────────────────────────────────────────┐
│              Production → QA Sync Flow (Mirroring)                      │
└─────────────────────────────────────────────────────────────────────────┘

1. Export Phase
   ┌──────────────────────────┐
   │ Production Superset      │  Current production state
   │ bi.ol.mit.edu            │  - 76+ datasets
   └────────┬─────────────────┘  - 100+ charts
            │                    - 18+ dashboards
            ▼
   ┌──────────────────────────┐
   │ ol-superset export       │  Export via sup CLI
   │   --from                 │  - Fetches ALL assets (pagination)
   │   superset-production    │  - Includes dependencies
   └────────┬─────────────────┘  - OAuth authentication
            │
            ▼
   ┌──────────────────┐
   │  assets/ folder  │  YAML files on disk
   │  (Git tracked)   │  - datasets/
   └────────┬─────────┘  - charts/
            │           - dashboards/
            ▼           - databases/

2. Sync Phase (with UUID Mapping)
   ┌──────────────────────────┐
   │ ol-superset sync         │  Automated push to QA
   │   superset-production    │  - Auto DB UUID mapping
   │   superset-qa            │  - CSRF token handling
   └────────┬─────────────────┘  - Continue on error
            │                    - Overwrite confirmation
            ▼
   ┌──────────────────────────┐
   │ QA Superset              │  Assets imported
   │ bi-qa.ol.mit.edu         │  - All datasets synced
   │                          │  - All charts synced
   └──────────────────────────┘  - All dashboards synced

Benefits:
  ✓ Fully automated (no manual import)
  ✓ Database UUID mapping handled automatically
  ✓ OAuth authentication with CSRF tokens
  ✓ Pagination fetches ALL assets
  ✓ Continue-on-error for resilience
  ✓ Git tracking for version control
```

## Comparison

| Aspect | QA → Production (Promote) | Production → QA (Sync) |
|--------|---------------------------|------------------------|
| **Primary Use** | Dashboard development | Regular sync, backup |
| **Direction** | QA first, then production | Production first, then QA |
| **Testing** | Changes tested in QA first | Production is source of truth |
| **Frequency** | Per dashboard change | Weekly/as needed |
| **Safety** | Multiple guardrails | Standard confirmation |
| **Risk** | Medium (production impact) | Low (safe to overwrite QA) |
| **Command** | `ol-superset promote` | `ol-superset sync` |

## CLI Command Reference

### Core Commands

| Command | Purpose | Key Options |
|---------|---------|-------------|
| `export` | Export all assets from any instance | `--from`, `--output-dir` |
| `validate` | Validate asset YAML files | `--assets-dir` |
| `sync` | Sync assets between instances | `--yes`, `--dry-run` |
| `promote` | Promote QA → Production with safety checks | `--force`, `--dry-run`, `--skip-validation` |

### Detailed Examples

```bash
# Export Operations
ol-superset export                                    # Export from production
ol-superset export --from superset-qa                 # Export from QA
ol-superset export --from superset-qa -o /tmp/backup  # Custom output directory

# Validation
ol-superset validate                                  # Validate default assets/
ol-superset validate --assets-dir /tmp/backup         # Validate custom directory

# Sync Operations
ol-superset sync superset-production superset-qa      # Standard sync
ol-superset sync superset-production superset-qa -y   # Skip confirmation
ol-superset sync superset-production superset-qa -n   # Dry-run (preview)

# Promotion (QA → Production)
ol-superset promote                                   # Full promotion workflow
ol-superset promote --dry-run                         # Preview promotion
ol-superset promote --force                           # Skip safety checks (DANGEROUS)
```

## Production Safety Features

### `promote` Command Guardrails

1. **Directional Lock**: Only allows QA → Production (hardcoded)
2. **Pre-flight Validation**: Runs `validate` automatically
3. **Git Status Check**: Warns about uncommitted changes
4. **Explicit Confirmation**: Requires typing "PROMOTE" (case-sensitive)
5. **Manifest Generation**: Creates record of what was promoted
6. **Dry-Run Mode**: Preview changes without applying

### `sync` Command Protections

1. **Production Warning**: Clear alert when targeting production
2. **Strong Confirmation**: Requires "SYNC TO PRODUCTION" for production targets
3. **Standard Confirmation**: Yes/no prompt for all other targets
4. **Dry-Run Mode**: Preview changes without applying
5. **Skip Confirmation**: `--yes` flag for automation

## Technical Details

### Authentication

- Uses OAuth2 with PKCE flow for self-hosted Superset
- CSRF tokens automatically fetched and included in POST requests
- Credentials configured via `sup instance` commands
- See `~/.sup/config.yml` for instance configurations

### Database UUID Mapping

When syncing between environments, database UUIDs differ:
- **Production Trino**: `8702691f-d666-4dac-943b-9382c02233e3`
- **QA Trino**: `9a22a54c-8b2f-4c66-a866-3f23812ec929`

The UUID mapping system:
1. Fetches database list from target instance
2. Matches by database name (e.g., "Trino")
3. Rewrites all `database_uuid` references in assets
4. Updates database config files with target UUIDs

This is handled automatically by both `sync` and `promote` commands.

### Pagination

The export command fetches ALL assets via pagination:
- Default page size: 100 items
- Automatically loops through all pages
- Ensures no assets are missed

### Asset Counts

Typical production export:
- **76+ datasets** (physical + virtual)
- **100+ charts** (all visualization types)
- **18+ dashboards** (published only)
- **2 databases** (Trino + Superset Metadata DB)

### Error Handling

- `--continue-on-error`: Import continues if individual assets fail
- Charts with invalid query_context may fail but won't block others
- Database connection validation warnings are non-blocking
- Failed assets are logged for review

## Security Considerations

1. **Database Credentials**: Never exported; configured per environment
2. **OAuth Tokens**: Cached in memory only, never on disk
3. **CSRF Protection**: Fresh tokens fetched for each import operation
4. **Git Tracking**: Only YAML definitions tracked; no sensitive data
5. **Audit Trail**: All changes tracked in git with commit messages
6. **Production Access**: Protected by multiple confirmation layers

## Troubleshooting

### "Assets not found" error
→ Run `ol-superset export --from <instance>` first to export assets

### "Command not found: ol-superset"
→ Ensure virtual environment is activated: `cd src/ol_superset && source .venv/bin/activate`
→ Or install with: `cd src/ol_superset && uv sync`

### "Could not extract database information"
→ JSON parsing fallback handles this automatically via regex extraction

### "Failed to import chart: query_context JSON not valid"
→ Non-blocking error, import continues with other charts

### Import shows fewer assets than exported
→ Use `sup <resource> list --limit 200` to see all assets

### "Cannot connect to database" warning
→ Expected during import; connection validation is non-blocking

## Typical Workflows

### Weekly Production Backup

```bash
cd src/ol_superset

# Export and commit
ol-superset export
git diff assets/
git add assets/
git commit -m "Weekly Superset production backup"
git push
```

### Develop New Dashboard

```bash
# 1. Create in QA UI (https://bi-qa.ol.mit.edu)

# 2. Export from QA
ol-superset export --from superset-qa

# 3. Validate
ol-superset validate

# 4. Review and commit
git diff assets/
git add assets/
git commit -m "Add new enrollment dashboard"
git push

# 5. Promote to production
ol-superset promote
```

### Mirror Production to QA

```bash
# 1. Export from production
ol-superset export --from superset-production

# 2. Review changes
git diff assets/

# 3. Sync to QA
ol-superset sync superset-production superset-qa
```

## Future Enhancements

Potential improvements for this workflow:

1. ✅ **Automated Transformations**: Database UUID mapping implemented
2. ✅ **API-Based Import**: Using sup CLI with OAuth + CSRF
3. ✅ **Pagination**: Fetches all assets automatically
4. ✅ **Unified CLI**: Single `ol-superset` command with subcommands
5. ✅ **Production Safety**: Multiple guardrails for production deployments
6. ⏳ **CI/CD Integration**: GitHub Actions to automate sync on schedule
7. ⏳ **Diff Viewer**: Enhanced diff tool for meaningful dashboard changes
8. ⏳ **Rollback Automation**: One-command rollback to previous version
