{% macro varchar_array_column(relation, column_name) %}
    {#
        `column_name` of `relation` as an array of varchar, null when it is empty.

        A Postgres text[] column lands differently per loader: Airbyte writes an
        Iceberg list, while ol_dlt's sql_database sources write the array's JSON text
        (e.g. '["career-growth"]'). The loader can differ by lake (MIT Learn's
        profiles_profile is dlt-loaded in QA and Airbyte-loaded in production), so
        this reads the column's type from the relation rather than from the lake.

        A column whose type is not an array is parsed as JSON text. At parse time
        (execute is false) and in a unit test, whose fixture gives the documented
        array type, the column is taken as an array.
    #}
    {%- set text_columns = [] -%}
    {%- if execute and model.resource_type != 'unit_test' -%}
        {%- for column in adapter.get_columns_in_relation(relation) if column.name | lower == column_name | lower -%}
            {%- set data_type = column.data_type | lower -%}
            {%- if not (data_type.startswith('array') or data_type.endswith('[]')) -%}
                {%- do text_columns.append(column.name) -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}
    {%- set array_expr = json_extract_varchar_array(column_name, "'$'") if text_columns else column_name -%}
    {{ null_if_empty_array(array_expr) }}
{% endmacro %}
