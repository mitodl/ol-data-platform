# Local dbt Quick Reference Card

## One-Time Setup (15 minutes)

```bash
./bin/setup-local-dbt.sh
# Follow prompts to register Iceberg sources
```

## Daily Commands

### Build Models
```bash
cd src/ol_dbt

# Single model
uv run dbt run --select my_model --target dev_local

# Multiple models
uv run dbt run --select tag:mitlearn --target dev_local

# Downstream dependencies
uv run dbt run --select my_model+ --target dev_local

# Upstream dependencies
uv run dbt run --select +my_model --target dev_local
```

### Test Models
```bash
# Test specific model
uv run dbt test --select my_model --target dev_local

# Test all
uv run dbt test --target dev_local
```

### Compile SQL
```bash
# Check compiled SQL without running
uv run dbt compile --select my_model --target dev_local

# View compiled SQL
cat target/compiled/open_learning/models/path/to/my_model.sql
```

## Targets Comparison

| Target | Use Case | Speed | Cost |
|--------|----------|-------|------|
| `dev_local` | Daily dev ✅ | Fast | $0 |
| `dev` | Legacy fallback | Slow | High |
| `production` | CI/CD only | Fast | Medium |

## Troubleshooting

### AWS credential errors
```bash
aws sts get-caller-identity  # Verify credentials
./bin/setup-local-dbt.sh     # Re-run setup
```

### Missing tables
```bash
# Register missing layer
uv run python bin/register-glue-sources.py --database ol_warehouse_production_staging
```

### Function not found errors
- Check if adapter dispatch macro exists
- Add new macro following `macros/cast_timestamp_to_iso8601.sql` pattern

### Performance issues
- First run is slower (S3 metadata cache)
- Subsequent runs are faster
- Consider increasing memory in `profiles.yml`

## Storage Management

```bash
# Check database size
du -sh ~/.ol-dbt/local.duckdb

# Clean up local DuckDB (re-register after)
rm -rf ~/.ol-dbt/
./bin/setup-local-dbt.sh
```

## Cleanup Trino Dev Schemas

**Using dbt run-operation (Recommended)**:
```bash
cd src/ol_dbt

# Drop schemas with your suffix (dry run first to preview)
uv run dbt run-operation trino__drop_old_relations \
  --args "{dry_run: true}" --target dev_production

# Execute cleanup
uv run dbt run-operation trino__drop_old_relations --target dev_production

# Drop all schemas with prefix
uv run dbt run-operation trino__drop_schemas_by_prefixes \
  --args "{prefixes: ['ol_warehouse_production_myname']}" \
  --target dev_production
```

**Using Python script** (more detailed output):
```bash
# Dry run
python bin/cleanup-dev-schemas.py --target dev_production

# Execute (requires confirmation)
python bin/cleanup-dev-schemas.py --target dev_production --execute
```

## Getting Help

1. Check `docs/LOCAL_DEVELOPMENT.md`
2. Review error messages carefully
3. Ask in #data-platform Slack

## Cost Savings Per Developer

| Metric | Old (Trino) | New (DuckDB) | Savings |
|--------|-------------|--------------|---------|
| Setup | 2-3 hours | 15 minutes | 88% |
| Build time | 5-15 min | 10-30 sec | 95% |
| Storage | 100+ GB | 1-10 GB | 90% |
| Monthly cost | $250-500 | $0 | 100% |

---
**Remember**: Use `--target dev_local` for all local development!
