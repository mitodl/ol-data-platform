{% macro date_parse(date_string, format_string) %}
  {{ return(adapter.dispatch('date_parse', 'open_learning')(date_string, format_string)) }}
{% endmacro %}

{% macro trino__date_parse(date_string, format_string) %}
  date_parse({{ date_string }}, {{ format_string }})
{% endmacro %}

{% macro duckdb__date_parse(date_string, format_string) %}
  {# DuckDB uses strptime instead of date_parse #}
  {# Also need to convert format string from Trino format to DuckDB format #}
  {# Trino: %i for minutes, DuckDB: %M for minutes #}
  {# This is a simplified version - may need more format conversions #}
  strptime({{ date_string }}, replace(replace({{ format_string }}, '%i', '%M'), '%s', '%S'))
{% endmacro %}

{% macro default__date_parse(date_string, format_string) %}
  {{ return(trino__date_parse(date_string, format_string)) }}
{% endmacro %}
