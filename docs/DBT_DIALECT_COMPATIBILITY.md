# dbt Cross-Database Dialect Compatibility

This document describes SQL dialect differences between the engines the dbt project
runs on, and how models stay portable across them.

## Overview

The ol-data-platform dbt project runs on three engines:
- Trino (Starburst Galaxy) builds the production warehouse today, and stays the
  production engine until the M3 cutover.
- StarRocks (self-hosted) already serves Superset, ol-analytics-api, MIT Learn's
  warehouse pull and the `b2b_analytics` materialized views, and will replace Trino
  as the only query engine. See [STARROCKS_MIGRATION.md](STARROCKS_MIGRATION.md) and
  [ol-infrastructure ADR 0012](https://github.com/mitodl/ol-infrastructure/blob/main/docs/adr/0012-trino-to-starrocks-cutover.md).
- DuckDB, for local development without cloud costs.

Until the cutover a model has to build on Trino, and should build on StarRocks and
DuckDB.

## The rule: dispatch engine-specific SQL through a macro

A model must not call a function whose name or behavior differs between engines.
Each such function has a macro under `src/ol_dbt/macros/` (most are in
`cross_db_functions.sql`) that calls `adapter.dispatch('<name>', 'open_learning')`
and carries a `starrocks__` and usually a `duckdb__` implementation. In most of them
the Trino body is `default__`. A few (e.g. `json_query_string`, `date_parse`,
`date_diff`) define `trino__` and have `default__` fall back to it.

Model and snapshot SQL in `models/` and `snapshots/` has no direct calls left to
`json_query`, `from_iso8601_timestamp`, `array_join`, `regexp_like`, `element_at` or
`cardinality`. Every use goes through a macro.

## Ascending sort keys say `nulls last`

An ascending ORDER BY puts NULLs last on Trino and DuckDB and first on StarRocks, so
`row_number() over (order by created_on)` picks a NULL `created_on` as row 1 on
StarRocks and `array_agg(x order by x)` starts with its NULLs. Descending keys put
NULLs last on all three. StarRocks' `SORT_NULLS_LAST` sql_mode is not used, because it
makes NULLs sort first on a descending key.

Every ascending key in a window's ORDER BY or an ordered aggregate (`array_agg`,
`group_concat`, the `order_by` argument of `array_agg_filter`) therefore ends in
`nulls last`, whether or not the column can be NULL:

```sql
row_number() over (partition by user_id order by created_on nulls last, id nulls last)
array_agg(tag order by tag nulls last)
{{ array_agg_filter('label', "kind = 'skill'", order_by='label nulls last') }}
```

`ol-dbt validate` (the `null_ordering` check, run in dbt PR CI) fails on an ascending
key with no NULLS clause. A query's own ORDER BY, top-level or in a subquery, is not
checked. It orders output rows, unless a LIMIT follows it, and then it needs `nulls last`
too.

## Examples

These were the first macros written, and each is applied across the models:

1. **`json_query_string(column, path)`** - Extract JSON string values
   - Trino: `json_query(col, 'lax $.path' omit quotes)`
   - DuckDB: `json_extract_string(col, '$.path')`, NULL for malformed JSON
   - StarRocks: `get_json_string(col, '$.path')`

2. **`from_iso8601_timestamp(timestamp_str)`** - Parse ISO8601 timestamps
   - Trino: `from_iso8601_timestamp(str)`, appending `Z` to zone-less strings so
     they parse as UTC
   - DuckDB: `cast(str as timestamp)`
   - StarRocks: strip any zone suffix and the `T`, then `cast(str as datetime)`

3. **`array_join(array, delimiter)`** - Join array elements
   - Trino: `array_join(arr, delim)` (native)
   - DuckDB: `array_to_string(arr, delim)`
   - StarRocks: `array_join(arr, delim)`

4. **`regexp_like(string, pattern)`** - Regex matching
   - Trino: `regexp_like(str, pattern)` (native)
   - DuckDB: `regexp_matches(str, pattern)`
   - StarRocks: `str regexp pattern`

5. **`element_at_array(array, index)`** - Array element access
   - Trino: `element_at(arr, idx)` with 1-based indexing
   - DuckDB: `(arr)[idx]` with 1-based indexing
   - StarRocks: `arr[idx]` with 1-based indexing

6. **`array_length(array)`** - Number of elements in an array
   - Trino: `cardinality(arr)` (native)
   - DuckDB: `len(arr)` (DuckDB's `cardinality()` only accepts maps)
   - StarRocks: `array_length(arr)` (`cardinality()` only exists from v3)

Many more exist (JSON extraction and construction, date formatting and parsing,
regex replace and split, unnesting, `try_cast`, hashing). Search `macros/` before
writing a new one.

These replace the bare aggregate, array and string calls:

7. **`arbitrary(expr)`** - Any non-null value in the group
   - Trino: `arbitrary(x)` (native)
   - DuckDB: `any_value(x)` (DuckDB's `arbitrary()` is `first()` and can return NULL)
   - StarRocks: `any_value(x)` (no `arbitrary()`)
   - `marts__micromasters_summary_timeseries` still calls `arbitrary()` directly

8. **`approx_percentile(expr, percentage)`** - Approximate percentile
   - Trino: `approx_percentile(x, p)`, a value of `x`'s type
   - DuckDB: `approx_quantile(x, p)`
   - StarRocks: `percentile_approx(x, p)`, interpolated and a double, so values can differ

9. **`split_part_or_null(string, delimiter, index)`** - One field of a split string
   - Trino: `split_part(s, d, i)`, NULL past the last field
   - DuckDB, StarRocks: count the fields first. StarRocks' `split_part()` returns `''` past
     the last field and for a NULL string; DuckDB's returns `''` past the last field

10. **`array_sort(array)`** - Ascending sort with NULL elements last
    - Trino: `array_sort(arr)` (native)
    - StarRocks: sorts the non-null elements and appends the NULLs (its `array_sort()` puts
      NULLs first)

11. **`array_agg_filter(expr, condition, order_by, distinct)`** - `array_agg(...) filter (where ...)`
    - Trino, DuckDB: the `FILTER` clause (native)
    - StarRocks: aggregates `(condition, expr)` pairs and keeps the pairs whose condition is
      true. Its own `FILTER` rewrite (`array_agg_if`) puts a NULL in the array for every row
      that fails the condition, and fails to analyze with an `ORDER BY`

12. **`try_cast(expr, type)`**, **`date_diff(unit, date1, date2)`**, **`dbt.dateadd`** - existing
    macros, now used for every bare `try_cast`, `date_diff` and `date_add` in the models.
    StarRocks' `date_diff()` is `date1 - date2`, the reverse of Trino's.

## Known gaps

- SQL in YAML is not dispatched. `loaded_at_field` and test `field:` expressions
  are plain SQL strings, and some still use Trino's `from_iso8601_timestamp` directly
  (e.g. `_stg_mitxonline__models.yml`, `_stg_mitxpro__models.yml`,
  `_edxorg__discovery__sources.yml`).
- JSON paths with quoted keys: paths such as `$.image_metadata."image-alt"` need
  the inner quotes escaped: `'$.image_metadata.\"image-alt\"'`, as in
  `int__ocw__resources.sql` and `int__ocw__live_courses.sql`.
- Some StarRocks gaps are not dialect. Views cannot be created in a Glue-type
  Iceberg catalog, dbt-starrocks has no `merge` or `delete+insert`, and dbt cannot
  create a schema in the catalog without a local override. ADR 0012 lists each with
  the task that tracks it.

## Recommended Local Development Workflow

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
- Models whose DuckDB macro implementation is missing or diverges will fail

### Option 3: Hybrid Approach (Best for Most Cases)

Use production for heavy/complex models, local for rapid iteration:

```bash
# Test your specific changes locally
dbt run --target dev_local --select your_model

# Validate against production data periodically (Trino)
dbt run --target dev_production --select your_model --vars '{schema_suffix: your_username}'
```

### Checking a model on StarRocks

The local-dev StarRocks cluster from ol-infrastructure builds the layered warehouse
into its own Iceberg catalog:

```bash
ol-dbt starrocks build --env dev --select +your_model_name
```

See [LOCAL_LAKE.md](LOCAL_LAKE.md) for setting up that cluster and loading its raw
tables.

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

## Contributing

When adding new SQL transformations:

1. **Use macros** - Prefer `{{ json_query_string() }}` over raw `json_query()`, and
   add a dispatched macro for any engine-specific function that lacks one
2. **Check compatibility** - Build on DuckDB and StarRocks locally; the production
   build still runs on Trino
3. **Document** - Note if a model requires specific database features
4. **Scope engine-only models** - A model that only makes sense on StarRocks gets
   `+enabled: "{{ target.type == 'starrocks' }}"` and `+tags: ["starrocks"]`, as the
   `b2b_analytics` directory does in `dbt_project.yml`

## See Also

- [Trino to StarRocks migration for dbt authors](STARROCKS_MIGRATION.md)
- [Local Development Guide](LOCAL_DEVELOPMENT.md)
- [dbt Macros](../src/ol_dbt/macros/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Trino Documentation](https://trino.io/docs/current/)
- [StarRocks Documentation](https://docs.starrocks.io/)
