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
    against its source's columns and row count and left alone, so a run that
    failed part way can be repeated. A count that differs fails the run: the
    connections should be paused, so the source cannot have changed since the
    copy. A copy this run made is dropped first, since it was never accepted.

    Args:
        tables: list of {raw_table, exclude_columns}.
        execute_archive: false (the default) logs what would be copied and
            writes nothing.
-#}
{% macro archive_airbyte_raw_tables(tables, execute_archive=false) %}
    {#- Only a trailing `_<suffix>` is removed: suffix `qa` on dev_qa must
        leave ol_warehouse_qa intact. -#}
    {%- set suffix = '_' ~ var('schema_suffix', '') -%}
    {%- set raw_base = target.schema[:-(suffix | length)] if target.schema.endswith(suffix) else target.schema -%}
    {%- set raw_schema = raw_base ~ '_raw' -%}
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
        {%- set kept_names = [] -%}
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
                {%- do kept_names.append(name) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- if not kept -%}
            {{ exceptions.raise_compiler_error(
                "archive_airbyte_raw_tables: no columns to copy from " ~ source_relation
            ) }}
        {%- endif -%}
        {%- set identifier = 'airbyte_archive__' ~ table['raw_table'] -%}
        {%- set archive_relation = api.Relation.create(
            database=target.database, schema=history_schema, identifier=identifier, type='table'
        ) -%}
        {#- Asked for by name, not found in a schema listing: Galaxy lists a
            table created seconds earlier late, so a re-run soon after a copy
            would miss it and the CTAS would fail on the existing table. -#}
        {%- set existing_names = adapter.get_columns_in_relation(archive_relation) | map(attribute='name') | map('lower') | list -%}
        {%- set exists = existing_names | length > 0 -%}
        {%- set dropped_text = (", leaving out " ~ dropped | join(', ')) if dropped else "" -%}
        {%- if not execute_archive -%}
            {{ log(source_relation ~ " -> " ~ archive_relation ~ ": " ~ kept | length ~ " column(s)" ~ dropped_text
                ~ (" (exists, would be checked)" if exists else ""), info=True) }}
        {%- else -%}
            {%- if not exists -%}
                {%- do adapter.create_schema(archive_relation) -%}
                {%- set select_sql -%}
                    select {{ kept | join(', ') }} from {{ source_relation }}
                {%- endset -%}
                {%- do run_query(adapter.dispatch('archive_create_table_as')(archive_relation, select_sql)) -%}
                {%- set ns.created = ns.created + 1 -%}
            {%- else -%}
                {#- An archive made under other exclusions (a pattern or an
                    exclude_columns entry added since) is not this copy. -#}
                {%- if existing_names | sort != kept_names | sort -%}
                    {{ exceptions.raise_compiler_error(
                        "archive_airbyte_raw_tables: " ~ archive_relation ~ " exists with columns "
                        ~ existing_names | sort | join(', ') ~ " but this copy would keep "
                        ~ kept_names | sort | join(', ') ~ ". Inspect it before deciding which is right."
                    ) }}
                {%- endif -%}
                {%- set ns.verified = ns.verified + 1 -%}
            {%- endif -%}
            {%- set source_rows = archive_row_count(source_relation) -%}
            {%- set archive_rows = archive_row_count(archive_relation) -%}
            {%- if source_rows != archive_rows -%}
                {#- A copy this run made is not an archive yet, so it is
                    dropped and the next run copies again. One that existed
                    before this run is left for someone to inspect. -#}
                {%- if not exists -%}
                    {%- do adapter.drop_relation(archive_relation) -%}
                {%- endif -%}
                {{ exceptions.raise_compiler_error(
                    "archive_airbyte_raw_tables: " ~ archive_relation ~ " has " ~ archive_rows ~ " rows and "
                    ~ source_relation ~ " has " ~ source_rows
                    ~ (". It existed before this run and was left as is." if exists else ". The copy was dropped.")
                    ~ " Check that every production Airbyte connection of the unit is paused."
                ) }}
            {%- endif -%}
            {{ log(source_relation ~ " -> " ~ archive_relation ~ ": " ~ archive_rows ~ " rows"
                ~ (" (already archived)" if exists else "") ~ dropped_text, info=True) }}
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


{% macro default__archive_create_table_as(relation, sql) %}
    create table {{ relation }} as {{ sql }}
{% endmacro %}


{#- History tables are Parquet, as dbt_project.yml sets for snapshots on Trino. -#}
{% macro trino__archive_create_table_as(relation, sql) %}
    create table {{ relation }} with (format = 'PARQUET') as {{ sql }}
{% endmacro %}
