# Trino to StarRocks migration for dbt authors

The Starburst Galaxy agreement is not being renewed, so StarRocks will replace Trino
as the only query engine for the warehouse. The decision record is
[ol-infrastructure ADR 0012](https://github.com/mitodl/ol-infrastructure/blob/main/docs/adr/0012-trino-to-starrocks-cutover.md).
This note covers the parts of it that change how dbt models in `src/ol_dbt` are
written. The ADR is the source for the target state and the schedule. This note
does not restate either beyond what a model author needs.

## Where things stand

Trino on Starburst Galaxy still builds the production warehouse. Dagster's `dbt`
assets in the lakehouse code location run dbt-trino against the `production`
profile and write each layer as Iceberg tables (and Trino views) registered in AWS
Glue. In QA, Dagster builds on StarRocks instead: the `starrocks_qa_lake` profile
writes the canonical `ol_warehouse_qa_<layer>` schemas through the QA cluster, so
the QA runs (the staging models and the `qa_scheduled` MIT Learn integration models)
surface StarRocks build failures before production moves. Nothing builds QA on
Trino any more.

StarRocks already runs in CI, QA and production, and several consumers read from it:
Superset, ol-analytics-api, MIT Learn's warehouse pull and the `b2b_analytics`
materialized views. It reads the lake through its Glue-backed Iceberg external
catalogs (`ol_data_lake_qa`, `ol_data_lake_production`). Besides the QA warehouse
above, it builds the `tag:starrocks` models (the `b2b_analytics` and
`b2b_learner_records` models) on a schedule in production, from the StarRocks-targeted
dbt assets in `dg_projects/lakehouse/lakehouse/assets/lakehouse/dbt_starrocks.py`.

Locally, `ol-dbt starrocks build --env dev` builds the layered warehouse into the
Iceberg catalog of the local-dev StarRocks cluster. See [LOCAL_LAKE.md](LOCAL_LAKE.md).

Against the QA or production lake, `--target starrocks_dev_qa` (with `--env qa`) or
`--target starrocks_dev_production` (with `--env production`) and
`--vars '{schema_suffix: <name>}'` build your own `ol_warehouse_<env>_<name>_<layer>`
copy. A suffix named after a layer (e.g. `dimensional` or `raw`) is refused, because
the copy would land in a canonical schema. The canonical targets, `starrocks_qa_lake` and `starrocks_production_lake`,
refuse a `schema_suffix` because their profiles drop it, and
`starrocks_production_lake` also refuses to run without `--allow-canonical-writes`.
So does `--env production` on its default `starrocks_production` target, which rebuilds
the `b2b_analytics` views ol-analytics-api serves, unless it runs with
`--vault-role readonly`.

## Decisions that affect models

The dbt-built layers stay Iceberg tables in Glue. StarRocks writes staging,
intermediate, dimensional, marts and reporting through the Glue external catalog
(dbt-starrocks `config(catalog=..., database=...)`), so pyiceberg and DuckDB can
still read every layer. Whether Trino can read StarRocks-written Iceberg tables,
which the production parallel run depends on, is not verified yet. StarRocks'
native `default_catalog` is only for serving materialized views such as
`b2b_analytics`. Glue also stays the write catalog
through the cutover. The Lakekeeper/Gravitino REST catalog is a separate project.

Views become tables. StarRocks cannot create a view in a Glue-type Iceberg catalog,
so the view-materialized models are being converted to tables for the cutover.
Don't add a new model with `materialized='view'` in a layer that lands in Glue.

Engine-specific SQL goes through a dispatched macro. A model must not call a
function whose name or semantics differ between Trino, StarRocks and DuckDB.
Use the macro in `src/ol_dbt/macros/` (most live in `cross_db_functions.sql`) that
dispatches through `adapter.dispatch('<name>', 'open_learning')`. If none exists,
add one with `starrocks__` and `duckdb__` implementations next to the Trino one.
In most of these macros the Trino body is `default__`. A few (e.g.
`json_query_string`, `date_parse`) define `trino__` and have `default__` call it.
[DBT_DIALECT_COMPATIBILITY.md](DBT_DIALECT_COMPATIBILITY.md) has the details.

dbt-starrocks 1.12 ships no `delete+insert` (or `merge`) strategy for external
catalogs, so the project implements `delete+insert` itself
(`macros/starrocks_incremental_delete_insert.sql`, #3189): a correlated DELETE on
`unique_key`, then an INSERT. DELETE works on Glue Iceberg through StarRocks and
writes position deletes. `INSERT OVERWRITE`
on Iceberg replaces only the partitions it writes and never truncates the table,
so don't use `insert_overwrite` expecting a full replace.

## Trino stays the production engine until M3

Until production consumers move and the Trino build is disabled, every model change
still has to build on Trino. A layered model that works only on StarRocks breaks
the production build. (StarRocks-only serving models opt out the way
`b2b_analytics` does in `dbt_project.yml`, with
`+enabled: "{{ target.type == 'starrocks' }}"` and `+tags: ["starrocks"]`.)
The milestones, using the ADR's 2027-02-28 contract-end column:

- M1, 2026-10-31: full warehouse green on QA StarRocks.
- M2, 2026-11-30: production parallel run, QA consumers repointed.
- M3, 2027-01-15: all production consumers on StarRocks, Trino build disabled.
- M4, 2027-02-13: Galaxy access revoked and its credentials removed.

If the contract turns out to end 2027-01-31, M3 and M4 move earlier. The ADR
has both columns.

After M4 the Trino profiles in `profiles.yml` (`qa`, `production`, `dev_qa`,
`dev_production`) and `ol-dbt local cleanup` stop working.
