{% macro cast_date_to_iso8601(column_name) %}
    to_iso8601(from_iso8601_date({{ column_name }}))
{% endmacro %}
