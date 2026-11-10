{#-
    Body of a snapshot that keeps the history of one raw table: one row per
    version of each record, and a `dbt_is_deleted` row when the record leaves
    the source.

    The loader's own columns (`_airbyte_*`, `_ab_*`, `_dlt_*`) are left out.
    They change on every re-read of an unchanged row, so with `check_cols: all`
    they would write a new version of every row on every run, and they differ
    between the Airbyte and dlt loads of the same table.

    A delete is only recorded for a table whose load reads every row (a replace
    load). An incremental load never drops a deleted row from raw, so the
    snapshot keeps seeing it.

    A load that leaves raw empty or short would otherwise make the snapshot
    write a delete row for every missing key, which cannot be undone, so the
    run first counts the current keys missing from the select
    (raw_history_delete_guard) and fails, writing nothing, when they are more
    than `max_delete_fraction` of the current keys.

    Args:
        raw_table: name of the table in the `ol_warehouse_raw_data` source.
        unique_key: the source table's primary key column(s).
        max_delete_fraction: the largest share of current keys one run may
            delete. Defaults to var('raw_history_max_delete_fraction', 0.1).
-#}
{% macro raw_history_snapshot(raw_table, unique_key, max_delete_fraction=none) %}
    {{ config(
        unique_key=unique_key,
        strategy='check',
        check_cols='all',
        hard_deletes='new_record'
    ) }}
    {%- set relation = source('ol_warehouse_raw_data', raw_table) -%}
    {%- set loader_prefixes = ['_airbyte_', '_ab_', '_dlt_'] -%}
    {%- set kept = [] -%}
    {%- if execute -%}
        {%- for column in adapter.get_columns_in_relation(relation) -%}
            {%- set name = column.name | lower -%}
            {%- set ns = namespace(loader_column=false) -%}
            {%- for prefix in loader_prefixes -%}
                {%- if name.startswith(prefix) -%}
                    {%- set ns.loader_column = true -%}
                {%- endif -%}
            {%- endfor -%}
            {%- if not ns.loader_column -%}
                {%- do kept.append(adapter.quote(column.name)) -%}
            {%- endif -%}
        {%- endfor -%}
        {#- `select *` would bring the loader's columns in and version every
            row on every load, so an empty column list must stop a run that
            writes. `compile` and `docs generate` also execute this, on targets
            that hold no raw tables (the docs workflow and the image build use
            DuckDB), so they keep the placeholder. -#}
        {%- if not kept and flags.WHICH in ['snapshot', 'build'] -%}
            {{ exceptions.raise_compiler_error(
                "raw_history_snapshot: no columns found for " ~ relation
            ) }}
        {%- endif -%}
    {%- endif -%}
    {%- set body -%}
    select {{ kept | join(', ') if kept else '*' }}
    from {{ relation }}
    {%- endset -%}
    {%- if execute and flags.WHICH in ['snapshot', 'build'] -%}
        {%- do raw_history_delete_guard(body, unique_key, max_delete_fraction) -%}
    {%- endif %}
    {{ body }}
{% endmacro %}


{#-
    Fail a raw history snapshot run that would mark too many records deleted.

    Counts the snapshot's current keys (not already marked deleted) that are
    missing from `body`, the select the snapshot reads. Raises, so the run
    writes nothing, when they exceed `max_delete_fraction` of the current keys.
    The first run of a snapshot has no current keys and is not checked.

    A deliberate mass delete (a loader cutover, or a table's first keyset run)
    passes when the run lists the snapshot in
    var('raw_history_allow_deletes'), e.g.
    --vars '{raw_history_allow_deletes: [snapshot_raw__x]}'. The count is
    logged either way, to compare with the expected key diff.
-#}
{% macro raw_history_delete_guard(body, unique_key, max_delete_fraction=none) %}
    {%- set target_relation = adapter.get_relation(
        database=this.database, schema=this.schema, identifier=this.identifier
    ) -%}
    {%- if target_relation is none -%}
        {{ return(none) }}
    {%- endif -%}
    {%- set keys = [unique_key] if unique_key is string else unique_key -%}
    {%- set limit = max_delete_fraction
        if max_delete_fraction is not none
        else var('raw_history_max_delete_fraction', 0.1) -%}
    {%- set guard_sql -%}
        with source_keys as (
            select distinct {{ keys | join(', ') }}
            from ({{ body }}) as source_rows
        )

        , current_keys as (
            select {{ keys | join(', ') }}
            from {{ target_relation }}
            where dbt_valid_to is null and coalesce(dbt_is_deleted, 'False') != 'True'
        )

        select
            count(*) as current_count
            , sum(case when source_keys.{{ keys[0] }} is null then 1 else 0 end)
                as missing_count
        from current_keys
        left join source_keys
            on
            {%- for key in keys %}
                {% if not loop.first %}and {% endif -%}
                current_keys.{{ key }} = source_keys.{{ key }}
            {%- endfor %}
    {%- endset -%}
    {%- set row = run_query(guard_sql).rows[0] -%}
    {%- set current_count = row[0] | int -%}
    {%- set missing_count = (row[1] or 0) | int -%}
    {%- set allowed = this.identifier in var('raw_history_allow_deletes', []) -%}
    {{ log(
        "raw_history_delete_guard " ~ this.identifier ~ ": " ~ missing_count
        ~ " of " ~ current_count ~ " current keys missing from the source"
        ~ (" (allowed for this run)" if allowed else ""),
        info=true
    ) }}
    {%- if not allowed and missing_count > current_count * limit -%}
        {{ exceptions.raise_compiler_error(
            "raw_history_delete_guard: " ~ this.identifier ~ " would mark "
            ~ missing_count ~ " of its " ~ current_count ~ " current records "
            ~ "deleted, more than " ~ limit ~ " of them. Nothing was written. "
            ~ "If the raw table is complete, rerun with --vars "
            ~ "'{raw_history_allow_deletes: [" ~ this.identifier ~ "]}'."
        ) }}
    {%- endif -%}
{% endmacro %}
