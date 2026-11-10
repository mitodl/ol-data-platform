{#-
    Return a predicate over `relation` (the raw table) that drops the records
    the source has deleted, or none when the table needs no filter or cannot
    be filtered now. raw_history_snapshot applies it so that a deleted record
    leaves the snapshot's input and gets a dbt_is_deleted row; staging is meant
    to apply the same predicate to its keyset tables.

    Which tables, and how, comes from raw_delete_filter_map(), generated from
    the ingestion inventory by `ol-dbt inventory history --write`
    (INGESTION_INVENTORY_SPEC.md §11). Decided 2026-10-29 on witan task
    tk-delete-capture-for-dlt-migrated-sources-reconcil-826b18.

    `keyset`: an incrementally loaded database table never loses a deleted
    row in raw, so ol_dlt.keyset reads the source's current keys into
    `<raw_table>__keyset` after each load. A raw row is kept when its key is in
    the keyset, or when the loader wrote it after the keyset read started, less
    var('raw_keyset_trust_margin_minutes', 60): such a row existed when the
    keyset was read, and the margin covers replica lag and clock skew. Inside
    the margin a delete waits one load; it is never recorded falsely.
    - no keyset table yet (the unit's keysets are not running): no filter.
    - an empty keyset: raises, since every key would read as deleted.
    - a keyset older than its max_age_hours (two load intervals): still
      applied, and logged. Every key it lacks was deleted before it was read,
      and every row loaded since is kept by the loader-time clause, so updates
      are recorded and later deletes wait. Running unfiltered instead would
      bring every key already recorded as deleted back as an open version,
      because raw still holds its row (owner decision 2026-11-10, replacing
      826b18 decision 3 for this filter). The keyset's freshness check is
      what alerts on a stale keyset.

    `latest_load`: a dlt merge feed that re-reads the whole source each load
    keeps rows the source dropped, so only the newest load's rows are kept.

    The checks that query the keyset run only for `snapshot` and `build`.
-#}
{% macro raw_delete_filter(raw_table, relation) %}
    {%- set spec = raw_delete_filter_map().get(raw_table) -%}
    {%- if spec is none or not execute -%}
        {{ return(none) }}
    {%- endif -%}
    {%- set loader_time = spec['loader_time_column'] -%}
    {%- if spec['kind'] == 'latest_load' -%}
        {{ return(loader_time ~ " = (select max(" ~ loader_time ~ ") from " ~ relation ~ ")") }}
    {%- endif -%}

    {%- set keyset = adapter.get_relation(
        database=relation.database,
        schema=relation.schema,
        identifier=relation.identifier ~ '__keyset'
    ) -%}
    {%- if keyset is none -%}
        {{ log("raw_delete_filter " ~ raw_table ~ ": no keyset table yet, so deletes are not filtered", info=true) }}
        {{ return(none) }}
    {%- endif -%}
    {%- if flags.WHICH in ['snapshot', 'build'] -%}
        {%- set check_sql -%}
            select
                count(*)
                , case
                    when max(_keyset_read_started_at)
                        < {{ dbt.dateadd('hour', -spec['max_age_hours'], dbt.current_timestamp()) }}
                    then 1 else 0
                end
            from {{ keyset }}
        {%- endset -%}
        {%- set row = run_query(check_sql).rows[0] -%}
        {%- if (row[0] or 0) | int == 0 -%}
            {{ exceptions.raise_compiler_error(
                "raw_delete_filter: " ~ keyset ~ " is empty, which would mark every "
                ~ raw_table ~ " record deleted"
            ) }}
        {%- endif -%}
        {%- if row[1] | int == 1 -%}
            {{ log(
                "raw_delete_filter " ~ raw_table ~ ": keyset older than "
                ~ spec['max_age_hours'] ~ " hours; deletes after its read wait for a fresh one",
                info=true
            ) }}
        {%- endif -%}
    {%- endif -%}
    {%- set loaded_at = dlt_load_id_timestamp(loader_time) if loader_time == '_dlt_load_id' else loader_time -%}
    {%- set trusted_after = dbt.dateadd(
        'minute', -var('raw_keyset_trust_margin_minutes', 60), 'max(_keyset_read_started_at)'
    ) -%}
    {{ return(
        "(" ~ spec['key'] ~ " in (select " ~ spec['key'] ~ " from " ~ keyset ~ ")"
        ~ " or " ~ loaded_at ~ " > (select " ~ trusted_after ~ " from " ~ keyset ~ "))"
    ) }}
{% endmacro %}


{#- dlt's `_dlt_load_id` is the load's start time in Unix seconds, as a string. -#}
{% macro dlt_load_id_timestamp(column) -%}
    {{ adapter.dispatch('dlt_load_id_timestamp', 'open_learning')(column) }}
{%- endmacro %}

{% macro default__dlt_load_id_timestamp(column) -%}
    from_unixtime(cast({{ column }} as double))
{%- endmacro %}

{% macro duckdb__dlt_load_id_timestamp(column) -%}
    to_timestamp(cast({{ column }} as double))
{%- endmacro %}

{% macro starrocks__dlt_load_id_timestamp(column) -%}
    cast(from_unixtime(cast(cast({{ column }} as double) as bigint)) as datetime)
{%- endmacro %}
