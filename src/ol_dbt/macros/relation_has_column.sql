{% macro relation_has_column(relation, column_name) %}
    {#
        Whether `relation` has a column named `column_name`, compared case-insensitively.
        True at parse time (execute is false), when there is no warehouse to ask.
    #}
    {%- if not execute -%}
        {%- do return(true) -%}
    {%- endif -%}
    {%- do return(
        column_name | lower in adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower')
    ) -%}
{% endmacro %}
