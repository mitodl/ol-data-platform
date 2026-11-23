{#
    dbt's default renders an added column's type as column.expanded_data_type,
    which for a StarRocks string column is varchar(<character_maximum_length>).
    StarRocks reports an Iceberg string column (and any external catalog's) as
    1073741824 long, and rejects a VARCHAR over 1048576, so every incremental
    model with on_schema_change='append_new_columns' that gains a string column
    fails: "Varchar size must be <= 1048576: 1073741824" (the tracking-log
    user_activity models gaining useractivity_load_id, QA, 2026-11-17).
    A string column longer than StarRocks allows, or with no length, is added
    as `string` instead. Every other type is rendered as before.
    Snapshots add columns through create_columns, whose default renders the same
    expanded_data_type, so starrocks__create_columns below gets the same mapping.
    Upstream: https://github.com/StarRocks/dbt-starrocks/issues/139. Remove these
    overrides once dbt-starrocks maps it.
#}
{% macro starrocks__alter_relation_add_remove_columns(relation, add_columns, remove_columns) %}
  {% set add_columns = add_columns or [] %}
  {% set remove_columns = remove_columns or [] %}
  {% set sql -%}
     alter {{ relation.type }} {{ relation.render() }}
            {% for column in add_columns %}
               add column {{ column.quoted }} {{ starrocks_column_ddl_type(column) }}{{ ',' if not loop.last }}
            {% endfor %}{{ ',' if add_columns and remove_columns }}
            {% for column in remove_columns %}
                drop column {{ column.quoted }}{{ ',' if not loop.last }}
            {% endfor %}
  {%- endset -%}
  {% do run_query(sql) %}
{% endmacro %}


{% macro starrocks__create_columns(relation, columns) %}
  {% for column in columns %}
    {% call statement() %}
      alter table {{ relation.render() }} add column {{ adapter.quote(column.name) }} {{ starrocks_column_ddl_type(column) }}
    {% endcall %}
  {% endfor %}
{% endmacro %}


{% macro starrocks_column_ddl_type(column) %}
  {#- StarRocks' VARCHAR limit, in bytes. -#}
  {%- set max_varchar_length = 1048576 -%}
  {%- if column.is_string() and (column.char_size is none or column.char_size > max_varchar_length) -%}
    {{ return('string') }}
  {%- endif -%}
  {{ return(column.expanded_data_type) }}
{% endmacro %}
