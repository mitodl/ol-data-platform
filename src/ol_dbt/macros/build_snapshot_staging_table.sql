{#-
    Drop a leftover `<snapshot>__dbt_tmp` before dbt creates the staging table.

    dbt-trino creates a snapshot's staging table with a plain `create table` and
    drops it only in `trino__post_snapshot`, after the MERGE commits. A snapshot
    interrupted in between (a preempted run worker, a failed MERGE, a Galaxy
    error) leaves the table behind, and every later run then fails with
    TABLE_ALREADY_EXISTS until someone drops it by hand. The staging table only
    ever holds the current run's change set, so dropping a stale one loses
    nothing.

    StarRocks has the same problem: its staging table is an ordinary table
    next to the snapshot, dropped in starrocks__post_snapshot. Before building
    it, a StarRocks run also repairs a run interrupted between the two
    statements of starrocks__snapshot_merge_sql, so the staging table is built
    against one open row per record, and it builds the table its own way (see
    starrocks_build_snapshot_staging_table).

    DuckDB keeps dbt's own behavior: dbt-duckdb names its staging table with a
    timestamp, so it never collides.
-#}
{% macro build_snapshot_staging_table(strategy, sql, target_relation) %}
    {%- if target.type in ('trino', 'starrocks') -%}
        {%- do drop_relation_if_exists(make_temp_relation(target_relation)) -%}
    {%- endif -%}
    {%- if target.type == 'starrocks' -%}
        {%- do starrocks_snapshot_drop_closed_open_rows(target_relation) -%}
        {{ return(starrocks_build_snapshot_staging_table(strategy, sql, target_relation)) }}
    {%- endif -%}
    {{ return(dbt.build_snapshot_staging_table(strategy, sql, target_relation)) }}
{% endmacro %}
