{% macro json_extract_scalar(json_col, json_path) %}
  {{ return(adapter.dispatch('json_extract_scalar', 'open_learning')(json_col, json_path)) }}
{% endmacro %}

{% macro trino__json_extract_scalar(json_col, json_path) %}
  json_extract_scalar({{ json_col }}, {{ json_path }})
{% endmacro %}

{% macro duckdb__json_extract_scalar(json_col, json_path) %}
  {# DuckDB uses json_extract or -> operator #}
  {# Convert $.field to .field for DuckDB #}
  json_extract({{ json_col }}, replace({{ json_path }}, '$', ''))
{% endmacro %}

{% macro default__json_extract_scalar(json_col, json_path) %}
  {{ return(trino__json_extract_scalar(json_col, json_path)) }}
{% endmacro %}
