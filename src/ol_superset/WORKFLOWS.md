# Superset Asset Management Workflows

This document describes the workflows for managing Superset assets between environments (Production, QA).

## Overview

We use the `sup` CLI tool for automated asset export/import with OAuth authentication and database UUID mapping. The workflow supports bidirectional sync between environments with proper transformation.

## Quick Start

### Export Assets from Any Environment

```bash
# Export from production (default)
./scripts/export_all.sh

# Export from QA
./scripts/export_all.sh superset-qa

# Export to custom directory
./scripts/export_all.sh superset-qa /tmp/qa-backup
```

### Sync Assets Between Environments

```bash
# Production → QA (most common)
./scripts/export_all.sh superset-production
./scripts/sync_assets.sh superset-production superset-qa

# QA → Production (for testing changes)
./scripts/export_all.sh superset-qa
./scripts/sync_assets.sh superset-qa superset-production
```

## Workflow Diagrams

### Primary Workflow: Production → QA (Mirroring)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                Production → QA Sync Flow (Automated)                    │
└─────────────────────────────────────────────────────────────────────────┘

1. Export Phase
   ┌──────────────────────────┐
   │ Production Superset      │  Current production state
   │ bi.ol.mit.edu            │  - 76+ datasets
   └────────┬─────────────────┘  - 100+ charts
            │                    - 18+ dashboards
            ▼
   ┌──────────────────────────┐
   │ ./scripts/               │  Automated export via sup CLI
   │ export_all.sh            │  - Fetches ALL assets (pagination)
   │ superset-production      │  - Includes dependencies
   └────────┬─────────────────┘  - OAuth authentication
            │
            ▼
   ┌──────────────────┐
   │  assets/ folder  │  YAML files on disk
   │  (Git tracked)   │  - datasets/
   └────────┬─────────┘  - charts/
            │           - dashboards/
            ▼           - databases/

2. UUID Mapping Phase (Automatic)
   ┌──────────────────────────┐
   │ map_database_uuids.py    │  Rewrites database UUIDs
   │                          │  - Prod Trino → QA Trino
   └────────┬─────────────────┘  - Handles UUID mismatches
            │
            ▼

3. Sync Phase
   ┌──────────────────────────┐
   │ ./scripts/               │  Automated push to QA
   │ sync_assets.sh           │  - CSRF token handling
   │ superset-production      │  - Overwrite confirmation
   │ superset-qa              │  - Continue on error
   └────────┬─────────────────┘
            │
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

### Alternative Workflow: QA → Production (Development)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                   QA → Production Promotion Flow                        │
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

2. Export Phase
   ┌──────────────────────────┐
   │ ./scripts/               │
   │ export_all.sh            │  Exports from QA
   │ superset-qa              │
   └────────┬─────────────────┘
            │
            ▼
   ┌──────────────────┐
   │  assets/ folder  │  YAML files on disk
   │  (Git tracked)   │
   └────────┬─────────┘
            │
            ▼

3. Review & Commit Phase
   ┌──────────────────┐
   │  git diff        │  Review changes
   └────────┬─────────┘
            │
            ▼
   ┌──────────────────┐
   │  git commit      │  Commit with description
   │  git push        │
   └────────┬─────────┘
            │
            ▼

4. Promotion Phase
   ┌──────────────────────────┐
   │ ./scripts/               │
   │ sync_assets.sh           │  Automated push to production
   │ superset-qa              │  (after confirmation)
   │ superset-production      │
   └────────┬─────────────────┘
            │
            ▼
   ┌──────────────────────────┐
   │ Production Superset      │  Assets imported automatically
   │ bi.ol.mit.edu            │  via sup CLI
   └──────────────────────────┘

Benefits:
  ✓ Changes tested before production
  ✓ Code review via git diff
  ✓ Audit trail in version control
  ✓ Automated promotion with confirmation
```

## Comparison

| Aspect | Production → QA | QA → Production |
|--------|----------------|-----------------|
| **Primary Use** | Regular sync, backup | Dashboard development |
| **Direction** | Production first, then QA | QA first, then production |
| **Testing** | Production is source of truth | Changes tested in QA first |
| **Frequency** | Weekly/as needed | Per dashboard change |
| **Automation** | Fully automated | Automated with confirmation |
| **Risk** | Low (safe to overwrite QA) | Medium (requires review) |

## Script Reference

### Core Scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| `export_all.sh` | Export all assets from any instance | `./scripts/export_all.sh [instance] [output-dir]` |
| `sync_assets.sh` | Sync assets between instances | `./scripts/sync_assets.sh <source> <target>` |
| `map_database_uuids.py` | Map database UUIDs for target environment | Called automatically by sync_assets.sh |

### Examples

```bash
# Export from production (default behavior)
./scripts/export_all.sh

# Export from QA to separate directory
./scripts/export_all.sh superset-qa /tmp/qa-backup

# Sync production to QA
./scripts/export_all.sh superset-production
./scripts/sync_assets.sh superset-production superset-qa

# Sync QA to production (after testing)
./scripts/export_all.sh superset-qa
./scripts/sync_assets.sh superset-qa superset-production
```

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

The `map_database_uuids.py` script:
1. Fetches database list from target instance
2. Matches by database name (e.g., "Trino")
3. Rewrites all `database_uuid` references in assets
4. Updates database config files with target UUIDs

### Pagination

The export scripts fetch ALL assets via pagination:
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

## Troubleshooting

### "Assets not found" error
→ Run `export_all.sh <instance>` first to export assets

### "Could not extract database information"
→ JSON parsing fallback handles this automatically via regex extraction

### "Failed to import chart: query_context JSON not valid"
→ Non-blocking error, import continues with other charts

### Import shows fewer assets than exported
→ Use `sup <resource> list --limit 200` to see all assets

### "Cannot connect to database" warning
→ Expected during import; connection validation is non-blocking

## Future Enhancements

Potential improvements for this workflow:

1. ✅ **Automated Transformations**: Database UUID mapping implemented
2. ✅ **API-Based Import**: Using sup CLI with OAuth + CSRF
3. ✅ **Pagination**: Fetches all assets automatically
4. ⏳ **CI/CD Integration**: GitHub Actions to automate sync on schedule
5. ⏳ **Diff Viewer**: Enhanced diff tool for meaningful dashboard changes
6. ⏳ **Rollback Automation**: One-command rollback to previous version
