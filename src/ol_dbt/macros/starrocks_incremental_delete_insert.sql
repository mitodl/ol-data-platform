{#
    dbt-starrocks 1.12.2 has no delete+insert strategy (its validator accepts
    default, insert_overwrite, dynamic_overwrite and microbatch), but the 21
    incremental models in this project use delete+insert on Trino and replace
    rows by unique_key. None of the adapter's strategies does that on an
    Iceberg table: default appends, so a reprocessed key is duplicated, and
    insert_overwrite / dynamic_overwrite are partition-scoped, so on an
    unpartitioned table they replace every row and on a partitioned one they
    replace whole partitions rather than keys. StarRocks can DELETE from an
    Iceberg table, so this implements delete+insert the way dbt-trino does.
    Upstream: https://github.com/StarRocks/dbt-starrocks/issues/80.

    The models keep incremental_strategy='delete+insert'. dbt-adapters refuses
    a builtin strategy name the adapter does not list as valid, so the
    validator below hands the materialization a custom name instead, and
    adapter.get_incremental_strategy_macro resolves that to
    get_incremental_starrocks_delete_insert_sql.

    The materialization passes the `keys` config as unique_key (incremental.sql
    in the adapter), not unique_key, so the strategy reads unique_key itself.

    The DELETE and the INSERT are two statements and two Iceberg commits, as
    they are on Trino. A reader between them sees the reprocessed keys
    missing, and a failed INSERT leaves them deleted until the next run.
    Rows whose key contains a NULL never match `=`, so each run appends them
    again, also as on Trino.
#}
{%- macro starrocks__validate_get_incremental_strategy(config) -%}
    {%- set strategy = config.get('incremental_strategy') or 'default' -%}
    {%- set valid_strategies = ['default', 'insert_overwrite', 'dynamic_overwrite', 'microbatch', 'delete+insert'] -%}
    {%- if strategy not in valid_strategies -%}
        {%- do exceptions.raise_compiler_error(
            "Invalid incremental strategy provided: " ~ strategy
            ~ ". Expected one of: " ~ valid_strategies | join(', ')
        ) -%}
    {%- endif -%}

    {%- if strategy == 'microbatch' -%}
        {%- do starrocks__validate_microbatch_config(config) -%}
    {%- endif -%}

    {%- if strategy == 'delete+insert' -%}
        {%- if not config.get('unique_key') -%}
            {%- do exceptions.raise_compiler_error(
                "The delete+insert incremental strategy requires unique_key on StarRocks."
            ) -%}
        {%- endif -%}
        {%- do return('starrocks_delete_insert') -%}
    {%- endif -%}
    {%- do return(strategy) -%}
{%- endmacro -%}

{%- macro get_incremental_starrocks_delete_insert_sql(arg_dict) -%}
    {%- set target = arg_dict["target_relation"] -%}
    {%- set source = arg_dict["temp_relation"] -%}
    {%- set incremental_predicates = arg_dict["incremental_predicates"] -%}
    {%- set dest_cols_csv = get_quoted_csv(arg_dict["dest_columns"] | map(attribute="name")) -%}
    {%- set unique_key = config.get('unique_key') -%}
    {%- set unique_keys = [unique_key] if unique_key is string else unique_key -%}

    {#-- An empty batch (e.g. no new tracking-log rows since the last run) has
         nothing to replace. Skipping the DELETE saves a scan of the target and
         an empty Iceberg commit. The temp table was created by CTAS in this
         run, so this read is not subject to the stale-snapshot lag that
         follows a write to an existing table. --#}
    {%- set batch_rows = run_query("select count(*) from " ~ source).columns[0].values()[0] -%}

    {%- if batch_rows > 0 -%}
        {%- call statement('delete_existing_keys') -%}
            delete from {{ target }}
            where exists (
                select 1
                from {{ source }} as dbt_internal_source
                where
                {%- for key in unique_keys %}
                    {{ target.identifier }}.{{ key }} = dbt_internal_source.{{ key }}
                    {{- " and" if not loop.last }}
                {%- endfor %}
            )
            {%- for predicate in incremental_predicates or [] %}
                and ({{ predicate }})
            {%- endfor %}
        {%- endcall -%}
    {%- endif -%}

    insert into {{ target }} ({{ dest_cols_csv }})
    select {{ dest_cols_csv }}
    from {{ source }}
{%- endmacro -%}
