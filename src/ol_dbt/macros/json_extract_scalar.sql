{% macro json_extract_scalar(json_col, json_path) %}
  {{ return(adapter.dispatch('json_extract_scalar', 'open_learning')(json_col, json_path)) }}
{% endmacro %}

{% macro trino__json_extract_scalar(json_col, json_path) %}
  json_extract_scalar({{ json_col }}, {{ json_path }})
{% endmacro %}

{% macro duckdb__json_extract_scalar(json_col, json_path) %}
  {# DuckDB: json_extract_string returns plain VARCHAR (no JSON quoting), equivalent to Trino json_extract_scalar #}
  json_extract_string({{ json_col }}, {{ json_path }})
{% endmacro %}

{% macro default__json_extract_scalar(json_col, json_path) %}
  {{ return(trino__json_extract_scalar(json_col, json_path)) }}
{% endmacro %}

{% macro starrocks__json_extract_scalar(json_col, json_path) %}
  {# StarRocks: get_json_string returns a plain string for scalar paths; a non-scalar path returns
     the serialized JSON structure rather than NULL (acceptable difference from Trino).
     get_json_string requires a VARCHAR JSON string, so cast in case json_col is JSON-typed
     (e.g. an element produced by starrocks__unnest_json_array/unnest_json_map). #}
  get_json_string(cast({{ json_col }} as varchar), {{ json_path }})
{% endmacro %}
