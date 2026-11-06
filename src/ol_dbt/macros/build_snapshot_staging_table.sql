{#-
    Drop a leftover `<snapshot>__dbt_tmp` before dbt creates the staging table.

    dbt-trino creates a snapshot's staging table with a plain `create table` and
    drops it only in `trino__post_snapshot`, after the MERGE commits. A snapshot
    interrupted in between (a preempted run worker, a failed MERGE, a Galaxy
    error) leaves the table behind, and every later run then fails with
    TABLE_ALREADY_EXISTS until someone drops it by hand. The staging table only
    ever holds the current run's change set, so dropping a stale one loses
    nothing.

    DuckDB and StarRocks keep dbt's own behavior: dbt-duckdb names its staging
    table with a timestamp, so it never collides.
-#}
{% macro build_snapshot_staging_table(strategy, sql, target_relation) %}
    {%- if target.type == 'trino' -%}
        {%- do drop_relation_if_exists(make_temp_relation(target_relation)) -%}
    {%- endif -%}
    {{ return(dbt.build_snapshot_staging_table(strategy, sql, target_relation)) }}
{% endmacro %}
