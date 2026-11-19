{#-
    Copy a unit's Airbyte-loaded raw tables into the history schema, as the
    last step before its first production dlt load replaces them
    (INGESTION_INVENTORY_SPEC.md §10.4). Run through
    `ol-dbt inventory archive <unit>`, which reads the table list from the
    unit's inventory file:

        dbt run-operation archive_airbyte_raw_tables --args '{tables: [...], execute_archive: true}'

    Each table is copied to `<history schema>.airbyte_archive__<raw table>`.
    The copy keeps the loader's columns (`_airbyte_extracted_at` is the only
    order an append table's versions have) and leaves out names matching
    raw_history_secret_patterns() and the table's `exclude_columns`, as the
    history snapshots do. It gets no grants, so it is read like raw.

    Archive tables are never rebuilt. One that exists already is checked
    against its source's row count and left alone, so a run that failed part
    way can be repeated. A count that differs fails the run: the connections
    should be paused, so the source cannot have changed since the copy.

    Args:
        tables: list of {raw_table, exclude_columns}.
        execute_archive: false (the default) logs what would be copied and
            writes nothing.
-#}
{% macro archive_airbyte_raw_tables(tables, execute_archive=false) %}
    {%- set raw_schema = target.schema.replace(var('schema_suffix', ''), '').rstrip('_') ~ '_raw' -%}
    {%- set history_schema = generate_schema_name('history', none) | trim -%}
    {%- set patterns = raw_history_secret_patterns() -%}
    {%- set ns = namespace(created=0, verified=0) -%}
    {{ log("Archiving " ~ tables | length ~ " table(s) from " ~ raw_schema ~ " into " ~ history_schema
        ~ ("" if execute_archive else " (dry run, nothing is written)"), info=True) }}
    {%- for table in tables -%}
        {%- set source_relation = adapter.get_relation(
            database=target.database, schema=raw_schema, identifier=table['raw_table']
        ) -%}
        {%- if source_relation is none -%}
            {{ exceptions.raise_compiler_error(
                "archive_airbyte_raw_tables: " ~ raw_schema ~ "." ~ table['raw_table'] ~ " does not exist"
            ) }}
        {%- endif -%}
        {%- set excluded = table.get('exclude_columns', []) | map('lower') | list -%}
        {%- set kept = [] -%}
        {%- set dropped = [] -%}
        {%- for column in adapter.get_columns_in_relation(source_relation) -%}
            {%- set name = column.name | lower -%}
            {%- set secret = namespace(found=false) -%}
            {%- for pattern in patterns if pattern in name -%}
                {%- set secret.found = true -%}
            {%- endfor -%}
            {%- if secret.found or name in excluded -%}
                {%- do dropped.append(name) -%}
            {%- else -%}
                {%- do kept.append(adapter.quote(column.name)) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- if not kept -%}
            {{ exceptions.raise_compiler_error(
                "archive_airbyte_raw_tables: no columns to copy from " ~ source_relation
            ) }}
        {%- endif -%}
        {%- set identifier = 'airbyte_archive__' ~ table['raw_table'] -%}
        {%- set existing = adapter.get_relation(
            database=target.database, schema=history_schema, identifier=identifier
        ) -%}
        {%- set archive_relation = api.Relation.create(
            database=target.database, schema=history_schema, identifier=identifier, type='table'
        ) -%}
        {%- set dropped_text = (", leaving out " ~ dropped | join(', ')) if dropped else "" -%}
        {%- if not execute_archive -%}
            {{ log(source_relation ~ " -> " ~ archive_relation ~ ": " ~ kept | length ~ " column(s)" ~ dropped_text
                ~ (" (exists, would be checked)" if existing is not none else ""), info=True) }}
        {%- else -%}
            {%- if existing is none -%}
                {%- do adapter.create_schema(archive_relation) -%}
                {%- set select_sql -%}
                    select {{ kept | join(', ') }} from {{ source_relation }}
                {%- endset -%}
                {%- do run_query(adapter.dispatch('archive_create_table_as')(archive_relation, select_sql)) -%}
                {%- set ns.created = ns.created + 1 -%}
            {%- else -%}
                {%- set ns.verified = ns.verified + 1 -%}
            {%- endif -%}
            {%- set source_rows = archive_row_count(source_relation) -%}
            {%- set archive_rows = archive_row_count(archive_relation) -%}
            {%- if source_rows != archive_rows -%}
                {{ exceptions.raise_compiler_error(
                    "archive_airbyte_raw_tables: " ~ archive_relation ~ " has " ~ archive_rows ~ " rows and "
                    ~ source_relation ~ " has " ~ source_rows
                    ~ ". Check that every production Airbyte connection of the unit is paused."
                ) }}
            {%- endif -%}
            {{ log(source_relation ~ " -> " ~ archive_relation ~ ": " ~ archive_rows ~ " rows"
                ~ (" (already archived)" if existing is not none else "") ~ dropped_text, info=True) }}
        {%- endif -%}
    {%- endfor -%}
    {%- if execute_archive -%}
        {{ log("Archived " ~ ns.created ~ " table(s); " ~ ns.verified ~ " already archived and matched.", info=True) }}
    {%- endif -%}
{% endmacro %}


{% macro archive_row_count(relation) %}
    {%- set result = run_query("select count(*) from " ~ relation) -%}
    {{ return(result.columns[0].values()[0] | int) }}
{% endmacro %}


{#- History tables are Parquet, as dbt_project.yml sets for snapshots on Trino. -#}
{% macro default__archive_create_table_as(relation, sql) %}
    create table {{ relation }} as {{ sql }}
{% endmacro %}


{% macro trino__archive_create_table_as(relation, sql) %}
    create table {{ relation }} with (format = 'PARQUET') as {{ sql }}
{% endmacro %}
