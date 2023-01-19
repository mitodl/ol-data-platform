{% macro cast_timestamp_to_iso8601(column_name) %}
    to_iso8601(from_iso8601_timestamp({{ column_name }}))
{% endmacro %}
