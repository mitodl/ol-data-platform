{% macro missing_columns_as_nulls(relation, columns) %}
    {#
        A typed null for each of `columns` ({name: SQL type}) that `relation` lacks, as
        ", cast(null as <type>) as <name>" lines to follow a select list. For a source
        whose writer adds columns only when it next writes (an Iceberg table under
        schema_update_mode "update"), so a model can select them before that write.
        Emits nothing at parse time (execute is false), like relation_has_column, and in
        a unit test, whose fixture for the input carries every column the source
        documents (and whose relation has no schema to look columns up in).
    #}
    {%- if not execute or model.resource_type == 'unit_test' -%}
        {%- do return('') -%}
    {%- endif -%}
    {%- set present = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
    {%- for column_name, data_type in columns.items() if column_name | lower not in present %}
    , cast(null as {{ data_type }}) as {{ column_name }}
    {%- endfor -%}
{% endmacro %}
