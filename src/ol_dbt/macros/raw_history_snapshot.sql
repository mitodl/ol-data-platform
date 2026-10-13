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

    Args:
        raw_table: name of the table in the `ol_warehouse_raw_data` source.
        unique_key: the source table's primary key column(s).
-#}
{% macro raw_history_snapshot(raw_table, unique_key) %}
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
    {%- endif %}
    select {{ kept | join(', ') if kept else '*' }}
    from {{ relation }}
{% endmacro %}
