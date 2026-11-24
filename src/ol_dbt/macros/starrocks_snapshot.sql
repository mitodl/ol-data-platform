{#-
    Snapshots on StarRocks, replacing dbt-starrocks 1.12.2's own.

    The adapter's snapshot code is a 2022 copy of dbt-core's and fails on
    dbt-core 1.12 before writing anything (StarRocks/dbt-starrocks#31, closed
    not_planned):
    - its snapshot_check_all_get_existing_columns takes 2 arguments, and
      dbt-core 1.12's check strategy passes 3. The adapter's macro shadows
      dbt-core's, so every check-strategy snapshot fails with "takes not more
      than 2 argument(s)";
    - its merge closes a version with a multi-table UPDATE, which StarRocks
      cannot run against an Iceberg table, and only for dbt_change_type
      'update', so a hard delete never closes the deleted record's version.

    This keeps dbt-core's materialization (as dbt-trino does) and replaces the
    MERGE, which StarRocks does not have, with one INSERT and one DELETE (see
    starrocks__snapshot_merge_sql).

    Unlike MERGE, the INSERT does not fail on duplicate keys in the snapshot's
    input: two input rows for one key write two open rows. Every snapshot here
    reads a source unique on its key (raw_history_snapshot dedupes its input).
-#}
{% materialization snapshot, adapter='starrocks' %}
    {%- if config.get('dbt_valid_to_current') -%}
        {%- do exceptions.raise_compiler_error(
            "dbt_valid_to_current is not supported for snapshots on StarRocks:"
            ~ " starrocks__snapshot_merge_sql treats a null dbt_valid_to as current."
        ) -%}
    {%- endif -%}
    {#-- A StarRocks node has no database: relations resolve in the session's
         catalog, the profile's. The node dict then has no 'database' key, and
         dbt-core's materialization (and snapshot_check_all_get_existing_columns)
         fail on model.database. None is what the session catalog means here. --#}
    {%- do model.update({'database': none}) -%}
    {{ return(materialization_snapshot_default()) }}
{% endmaterialization %}


{#-
    dbt-core 1.12's macro, restated so that it takes precedence over
    dbt-starrocks' 2-argument copy (a root-project macro wins over an adapter
    package's, and this one is not dispatched). Every adapter resolves this
    name, so it must stay identical to dbt-core's: Trino and DuckDB then build
    exactly as they would without it. Re-copy it on a dbt-core upgrade that
    changes the original (global_project/macros/materializations/snapshots/
    strategies.sql).
-#}
{% macro snapshot_check_all_get_existing_columns(node, target_exists, check_cols_config) -%}
    {%- if not target_exists -%}
        {#-- no table yet -> return whatever the query does --#}
        {{ return((false, query_columns)) }}
    {%- endif -%}

    {#-- handle any schema changes --#}
    {%- set target_relation = adapter.get_relation(database=node.database, schema=node.schema, identifier=node.alias) -%}

    {% if check_cols_config == 'all' %}
        {%- set query_columns = get_columns_in_query(node['compiled_code']) -%}

    {% elif check_cols_config is iterable and (check_cols_config | length) > 0 %}
        {#-- query for proper casing/quoting, to support comparison below --#}
        {%- set select_check_cols_from_target -%}
            {#-- N.B. The whitespace below is necessary to avoid edge case issue with comments --#}
            {#-- See: https://github.com/dbt-labs/dbt-core/issues/6781 --#}
            select {{ check_cols_config | join(', ') }} from (
                {{ node['compiled_code'] }}
            ) subq
        {%- endset -%}
        {% set query_columns = get_columns_in_query(select_check_cols_from_target) %}

    {% else %}
        {% do exceptions.raise_compiler_error("Invalid value for 'check_cols': " ~ check_cols_config) %}
    {% endif %}

    {%- set existing_cols = adapter.get_columns_in_relation(target_relation) | map(attribute = 'name') | list -%}
    {%- set ns = namespace() -%} {#-- handle for-loop scoping with a namespace --#}
    {%- set ns.column_added = false -%}

    {%- set intersection = [] -%}
    {%- for col in query_columns -%}
        {%- if col in existing_cols -%}
            {%- do intersection.append(adapter.quote(col)) -%}
        {%- else -%}
            {% set ns.column_added = true %}
        {%- endif -%}
    {%- endfor -%}
    {{ return((ns.column_added, intersection)) }}
{%- endmacro %}


{#-
    dbt-core's MERGE, as two statements: one INSERT that adds a closed copy of
    every version the run closes (its row with dbt_valid_to set) together with
    the run's new versions, then one DELETE of the open rows those copies
    replace. Each is one Iceberg commit. A run with nothing to close or insert
    writes nothing, since an empty commit still adds an Iceberg snapshot.

    The INSERT reads the closed versions from the target, because dbt-core's
    staging table does not carry them: an 'update' row holds the new values,
    and a 'delete' row holds nulls.

    The INSERT goes first so an interrupted run loses no history. It can leave
    a version both open and closed (same dbt_scd_id). The next run repairs that
    before it builds its staging table (starrocks_snapshot_drop_closed_open_rows,
    called from build_snapshot_staging_table), with the same DELETE this macro
    returns. Readers between the two statements see the closed and the open
    row of a changed record.
-#}
{% macro starrocks__snapshot_merge_sql(target, source, insert_cols) -%}
    {%- set columns = config.get('snapshot_table_column_names') or get_snapshot_table_column_names() -%}
    {%- set target_columns = adapter.get_columns_in_relation(target) | map(attribute='name') | list -%}
    {%- set quoted_target_columns = [] -%}
    {%- for column in target_columns -%}
        {%- do quoted_target_columns.append(adapter.quote(column)) -%}
    {%- endfor -%}
    {%- set source_columns = adapter.get_columns_in_relation(source) | map(attribute='name') | map('lower') | list -%}

    {%- set counts = run_query(
        "select"
        ~ " count(case when dbt_change_type in ('update', 'delete') then 1 end),"
        ~ " count(case when dbt_change_type = 'insert' then 1 end)"
        ~ " from " ~ source
    ).rows[0] -%}
    {%- set closing_count = counts[0] -%}
    {%- set insert_count = counts[1] -%}
    {%- do log(
        target ~ ": closing " ~ closing_count ~ " version(s), inserting "
        ~ insert_count ~ " version(s)", info=true
    ) -%}

    {%- if closing_count > 0 or insert_count > 0 -%}
        {%- call statement('snapshot_insert_versions') -%}
            insert into {{ target }} ({{ quoted_target_columns | join(', ') }})
            {%- if closing_count > 0 %}
            select
            {%- for column in target_columns %}
                {% if column | lower == columns.dbt_valid_to | lower -%}
                    dbt_internal_source.{{ adapter.quote(column) }}
                {%- else -%}
                    dbt_internal_dest.{{ adapter.quote(column) }}
                {%- endif -%}
                {{ "," if not loop.last }}
            {%- endfor %}
            from {{ target }} as dbt_internal_dest
            inner join {{ source }} as dbt_internal_source
                on dbt_internal_source.{{ columns.dbt_scd_id }} = dbt_internal_dest.{{ columns.dbt_scd_id }}
            where dbt_internal_dest.{{ columns.dbt_valid_to }} is null
                and dbt_internal_source.dbt_change_type in ('update', 'delete')
            {%- endif %}
            {%- if closing_count > 0 and insert_count > 0 %}
            union all
            {%- endif %}
            {%- if insert_count > 0 %}
            select
            {%- for column in target_columns %}
                {% if column | lower in source_columns -%}
                    dbt_internal_source.{{ adapter.quote(column) }}
                {%- else -%}
                    null
                {%- endif -%}
                {{ "," if not loop.last }}
            {%- endfor %}
            from {{ source }} as dbt_internal_source
            where dbt_internal_source.dbt_change_type = 'insert'
            {%- endif %}
        {%- endcall -%}
    {%- endif -%}

    {%- if closing_count > 0 -%}
        {{ starrocks_snapshot_drop_closed_open_rows_sql(target, columns) }}
    {%- else -%}
        {#-- The materialization runs this as its 'main' statement. --#}
        select 0
    {%- endif -%}
{%- endmacro %}


{#-
    Delete the open row of every version that also has a closed row: the
    second statement of starrocks__snapshot_merge_sql, and the repair for a run
    interrupted between its two statements.
-#}
{% macro starrocks_snapshot_drop_closed_open_rows_sql(target, columns) -%}
    delete from {{ target }}
    where {{ columns.dbt_valid_to }} is null
        and exists (
            select 1
            from {{ target }} as dbt_internal_closed
            where dbt_internal_closed.{{ columns.dbt_scd_id }} = {{ target.identifier }}.{{ columns.dbt_scd_id }}
                and dbt_internal_closed.{{ columns.dbt_valid_to }} is not null
        )
{%- endmacro %}


{% macro starrocks_snapshot_drop_closed_open_rows(target) %}
    {%- set columns = config.get('snapshot_table_column_names') or get_snapshot_table_column_names() -%}
    {%- set stale = run_query(
        "select count(*) from " ~ target ~ " as dbt_internal_open"
        ~ " where dbt_internal_open." ~ columns.dbt_valid_to ~ " is null"
        ~ " and exists (select 1 from " ~ target ~ " as dbt_internal_closed"
        ~ " where dbt_internal_closed." ~ columns.dbt_scd_id ~ " = dbt_internal_open." ~ columns.dbt_scd_id
        ~ " and dbt_internal_closed." ~ columns.dbt_valid_to ~ " is not null)"
    ).columns[0].values()[0] -%}
    {%- if stale > 0 -%}
        {%- do log(
            target ~ ": deleting " ~ stale ~ " open row(s) left by an interrupted run",
            info=true
        ) -%}
        {%- call statement('snapshot_repair_interrupted_run') -%}
            {{ starrocks_snapshot_drop_closed_open_rows_sql(target, columns) }}
        {%- endcall -%}
    {%- endif -%}
{% endmacro %}


{#-
    Build the snapshot's staging table with every column nullable.

    dbt-core's staging query gives a 'delete' row nulls in all the source
    columns (its `deletes` CTE selects them from the unmatched side of a left
    join). StarRocks 4.1.8 still types those columns NOT NULL when the source
    column is, so `create table ... as` the query fails with "NULL value in
    non-nullable column" on the first run that sees a delete. Reproduced on QA
    with the staging query alone (`select * from deletes`).

    So the table is created from the query's shape (an empty CTAS, read back
    with DESC), with no NOT NULL constraints, and then filled by INSERT. A
    VARCHAR(n) is declared as STRING: StarRocks refuses an explicit length
    above 1048576, and an Iceberg string has no length anyway. This rewrites
    DESC's type text rather than using starrocks_column_ddl_type, which maps
    only a top-level string and would keep the length of one nested in an
    ARRAY, MAP or STRUCT column.
-#}
{% macro starrocks_build_snapshot_staging_table(strategy, sql, target_relation) %}
    {%- set temp_relation = make_temp_relation(target_relation) -%}
    {%- set select = snapshot_staging_table(strategy, sql, target_relation) -%}

    {%- call statement('snapshot_staging_shape') -%}
        create table {{ temp_relation.include(database=False) }} as
        select * from (
            {{ select }}
        ) as dbt_internal_shape
        where false
    {%- endcall -%}
    {%- set described = run_query('desc ' ~ temp_relation.include(database=False)) -%}
    {%- do drop_relation_if_exists(temp_relation) -%}

    {%- call statement('build_snapshot_staging_relation') -%}
        create table {{ temp_relation.include(database=False) }} (
        {%- for row in described.rows %}
            {{ adapter.quote(row[0]) }} {{ modules.re.sub('(?i)varchar\\(\\d+\\)', 'string', row[1]) }}
            {{- "," if not loop.last }}
        {%- endfor %}
        )
    {%- endcall -%}
    {%- call statement('fill_snapshot_staging_relation') -%}
        insert into {{ temp_relation.include(database=False) }}
        {{ select }}
    {%- endcall -%}

    {%- do return(temp_relation) -%}
{% endmacro %}


{% macro starrocks__post_snapshot(staging_relation) %}
    {% do drop_relation_if_exists(staging_relation) %}
{% endmacro %}
