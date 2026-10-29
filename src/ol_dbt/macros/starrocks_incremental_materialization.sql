{#
    dbt-starrocks 1.12.2's incremental materialization builds an
    external-catalog model into `config.get('database') or target.schema`, not
    the schema the model resolves to, so with a per-folder +schema (staging,
    dimensional, ...) an incremental model lands in the profile's schema while
    every ref() to it points at the layer schema, and the downstream models
    fail with "Unknown table". The table materialization already uses
    target_relation.schema.
    Upstream: https://github.com/StarRocks/dbt-starrocks/issues/132, fixed by
    https://github.com/StarRocks/dbt-starrocks/pull/133 (merged 2026-10-15, not
    in a release as of 1.12.2). Delete this file once dbt-starrocks is bumped
    past a release that contains it.

    This is the 1.12.2 materialization verbatim (Apache-2.0, Copyright
    2021-present StarRocks, Inc.) apart from that one line. A materialization
    in the root project takes precedence over the adapter's.
#}
{%- materialization incremental, adapter='starrocks' -%}
    {%- set keys = config.get('keys', validator=validation.any[list]) -%}
    {%- set full_refresh_mode = (should_full_refresh()) -%}
    {%- set identifier = this.name -%}
    {%- set target_relation = api.Relation.create(
        identifier=identifier,
        schema=schema,
        database=database,
        type='table',
    ) -%}
    {%- set tmp_relation = make_temp_relation(this).incorporate(type='table') -%}

    {%- set catalog = config.get('catalog', target.catalog) -%}
    {%- set database = config.get('database') or target_relation.schema -%}

    {%- if catalog != 'default_catalog' and database -%}
      {%- set is_external = true -%}
    {%- endif -%}

    {%- if is_external -%}
      {%- set target_relation = adapter.Relation.create(
        database=catalog,
        schema=database,
        identifier=target_relation.identifier,
        type='table'
      ) -%}
      {%- set tmp_relation = adapter.Relation.create(
        database=catalog,
        schema=database,
        identifier=tmp_relation.identifier,
        type='table'
      ) -%}
    {%- endif -%}

    {%- set existing_relation = load_relation(target_relation) -%}
    {%- set incremental_strategy = starrocks__validate_get_incremental_strategy(config) -%}

    {{ drop_relation_if_exists(tmp_relation) }}

    {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}

    {{ run_hooks(pre_hooks) }}

    {%- if existing_relation is none -%}
      {%- call statement('main') -%}
          {{ starrocks__create_table_as(False, target_relation, compiled_code, is_external) }}
      {%- endcall -%}

    {%- elif existing_relation.is_view -%}
        {#-- Can't overwrite a view with a table, drop it before creating table --#}
        {{ log("Dropping relation " ~ target_relation ~ " because it is a view and this model is a table.") }}
        {%- do adapter.drop_relation(existing_relation) -%}
        {%- call statement('main') -%}
            {{ starrocks__create_table_as(False, target_relation, compiled_code, is_external) }}
        {%- endcall -%}

    {%- elif full_refresh_mode and is_external -%}
        {#-- External catalogs have no ALTER TABLE ... SWAP WITH, so the backup-and-exchange
             route below is unavailable: drop and recreate in place instead. --#}
        {%- do adapter.drop_relation(existing_relation) -%}
        {%- call statement('main') -%}
            {{ starrocks__create_table_as(False, target_relation, compiled_code, is_external) }}
        {%- endcall -%}

    {%- elif full_refresh_mode -%}
        {#-- First create a backup of the existing table and then exchange the new table with the backup --#}
        {%- set backup_identifier = existing_relation.identifier ~ "__dbt_backup" -%}
        {%- set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) -%}
        {%- do adapter.drop_relation(backup_relation) -%}

        {%- call statement('main') -%}
            {{ starrocks__create_table_as(False, backup_relation, compiled_code, is_external) }}
        {%- endcall -%}

        {%- do starrocks__exchange_relation(target_relation, backup_relation) -%}
    {% else %}
        {#-- Create the temp relation, either as a view or as a temp table --#}
        {%- call statement('create_tmp_relation') -%}
            {{ starrocks__create_table_as(True, tmp_relation, compiled_code, is_external) }}
        {%- endcall -%}

        {%- do adapter.expand_target_column_types(
            from_relation=tmp_relation,
            to_relation=target_relation
        ) -%}
        {#-- Process schema changes. Returns dict of changes if successful. Use source columns for upserting/merging --#}
        {%- set dest_columns = process_schema_changes(on_schema_change, tmp_relation, existing_relation) -%}
        {%- if not dest_columns %}
            {%- set dest_columns = adapter.get_columns_in_relation(existing_relation) -%}
        {%- endif -%}

        {#-- Get the incremental_strategy, the macro to use for the strategy, and build the sql --#}
        {%- set incremental_predicates = config.get('predicates', none) or config.get('incremental_predicates', none) -%}
        {%- set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) -%}
        {%- set strategy_arg_dict = ({'target_relation': target_relation, 'temp_relation': tmp_relation, 'unique_key': keys, 'dest_columns': dest_columns, 'incremental_predicates': incremental_predicates }) -%}

        {%- call statement('main') -%}
            {{ strategy_sql_macro_func(strategy_arg_dict) }}
        {%- endcall -%}
    {%- endif -%}

    {%- do drop_relation_if_exists(tmp_relation) -%}

    {{ run_hooks(post_hooks) }}

    {%- set target_relation = target_relation.incorporate(type='table') -%}

    {%- set should_revoke = should_revoke(existing_relation.is_table, full_refresh_mode) -%}
    {%- do apply_grants(target_relation, grant_config, should_revoke=should_revoke) -%}
    {%- do persist_docs(target_relation, model) -%}

    {{ return({'relations': [target_relation]}) }}
{%- endmaterialization -%}
