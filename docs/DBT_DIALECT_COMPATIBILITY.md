# dbt Cross-Database Dialect Compatibility

This document describes SQL dialect differences between Trino (production), DuckDB (local dev), and StarRocks (future), and the current state of compatibility work.

## Overview

The ol-data-platform dbt project runs on:
- **Trino** (production via Starburst Galaxy) - primary target
- **DuckDB** (local development) - for fast iteration without cloud costs
- **StarRocks** (planned future support) - potential alternative OLAP engine

## Compatibility Status

### ✅ Fully Compatible (Macros Implemented)

These functions have cross-database macros and work across all targets:

1. **`json_query_string(column, path)`** - Extract JSON string values
   - Trino: `json_query(col, 'lax $.path' omit quotes)`
   - DuckDB: `json_extract_string(col, '$.path')`
   - StarRocks: `get_json_string(col, '$.path')`
   - **Converted**: 433 instances across 51 files ✅

2. **`from_iso8601_timestamp(timestamp_str)`** - Parse ISO8601 timestamps
   - Trino: `from_iso8601_timestamp(str)` (native)
   - DuckDB: `cast(str as timestamp)`
   - StarRocks: `cast(str as datetime)`
   - **Status**: Macro created, not yet applied to models

3. **`array_join(array, delimiter)`** - Join array elements
   - Trino: `array_join(arr, delim)` (native)
   - DuckDB: `array_to_string(arr, delim)`
   - StarRocks: `array_join(arr, delim)`
   - **Status**: Macro created, not yet applied to models

4. **`regexp_like(string, pattern)`** - Regex matching
   - Trino: `regexp_like(str, pattern)` (native)
   - DuckDB: `regexp_matches(str, pattern)`
   - StarRocks: `str regexp pattern`
   - **Status**: Macro created, not yet applied to models

5. **`element_at_array(array, index)`** - Array element access
   - Trino: `element_at(arr, idx)` with 1-based indexing
   - DuckDB: `(arr)[idx]` with 1-based indexing
   - StarRocks: `arr[idx]` with 1-based indexing
   - **Status**: Macro created, not yet applied to models

### ⚠️ Partially Compatible (Known Issues)

6. **JSON with nested quotes** - Some JSON paths contain literal double quotes
   - Example: `$.image_metadata."image-alt"`
   - **Workaround**: Escape inner quotes: `$.image_metadata.\"image-alt\"`
   - **Status**: Fixed in 1 file, others may exist

### 🔴 Not Yet Compatible

These areas still have Trino-specific syntax that needs work:

- **Geospatial functions** (ST_*, to_spherical_geography, etc.)
- **Time zone conversions** (AT TIME ZONE)
- **IPADDRESS type** and related functions
- **JSON_OBJECT, JSON_ARRAY** constructors (different syntax)
- **Windowing differences** (frame clauses)
- **Some aggregate functions** (approx_distinct, etc.)

## Current Conversion Statistics

| Function Type | Total Uses | Converted | Remaining |
|--------------|------------|-----------|-----------|
| json_query | 433 | 433 ✅ | 0 |
| from_iso8601_timestamp | 89 | 0 | 89 |
| element_at (arrays) | 10 | 0 | 10 |
| regexp_like | 9 | 0 | 9 |
| array_join | 16 | 0 | 16 |

## Recommended Local Development Workflow

Given the extensive dialect differences and DuckDB memory constraints, we recommend:

### Option 1: Selective Local Development (Recommended)

Build only the models you're actively working on locally, fallback to Glue for everything else:

```bash
# Register all production Glue tables as views
ol-dbt local register

# Build only your target model and immediate dependencies
dbt run --target dev_local --select +your_model_name

# The smart ref() macro automatically uses Glue views for unavailable upstream models
```

**Benefits**:
- Fast iteration on your specific changes
- No need to build entire DAG locally
- Minimal memory usage
- Works around dialect incompatibilities

### Option 2: Layer-by-Layer Build

Build compatible layers incrementally:

```bash
# 1. Staging models (mostly 1:1 transformations, usually compatible)
dbt run --target dev_local --select staging.*

# 2. Intermediate models (may have dialect issues)
dbt run --target dev_local --select intermediate.* --exclude config.materialized:incremental

# 3. Marts (depends on intermediate success)
dbt run --target dev_local --select marts.*
```

**Known issues with this approach**:
- Memory errors on large models (even with 24GB limit)
- Dialect incompatibilities will cause failures
- Requires converting remaining 124 function calls

### Option 3: Hybrid Approach (Best for Most Cases)

Use production for heavy/complex models, local for rapid iteration:

```bash
# Test your specific changes locally
dbt run --target dev_local --select your_model

# Validate against production data periodically
dbt run --target dev_production --select your_model --vars '{schema_suffix: your_username}'
```

## Memory Management

DuckDB memory is configurable via `DBT_DUCKDB_MEMORY_LIMIT` environment variable:

```bash
# Set based on your system RAM (recommend 75% of available)
export DBT_DUCKDB_MEMORY_LIMIT=24GB  # For 32GB system
export DBT_DUCKDB_MEMORY_LIMIT=48GB  # For 64GB system
```

**Known memory-intensive models**:
- `int__mitxonline__course_structure` - Large JSON parsing and unnesting
- Models with many aggregations or window functions
- Models processing tracking logs (millions of rows)

## Next Steps for Full Compatibility

To complete DuckDB compatibility:

1. **Convert remaining function calls** (124 instances):
   ```bash
   # from_iso8601_timestamp: 89 instances
   # array_join: 16 instances
   # element_at: 10 instances
   # regexp_like: 9 instances
   ```

2. **Test and fix edge cases**:
   - Memory optimization for large models
   - Incremental model support
   - Complex JSON operations

3. **Document unsupported models**:
   - Create exclusion list for models that can't run locally
   - Add notes in model configs

4. **Performance tuning**:
   - Optimize memory-intensive transformations
   - Consider sampling strategies for local dev

## Contributing

When adding new SQL transformations:

1. **Check compatibility** - Test on both Trino and DuckDB if possible
2. **Use macros** - Prefer `{{ json_query_string() }}` over raw `json_query()`
3. **Document** - Note if a model requires specific database features
4. **Config tags** - Tag Trino-only models: `tags: ['trino_only']`

## See Also

- [Local Development Guide](LOCAL_DEVELOPMENT.md)
- [dbt Macros](../src/ol_dbt/macros/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Trino Documentation](https://trino.io/docs/current/)
