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
  {# StarRocks: get_json_string returns a plain string for scalar paths, but the serialized
     JSON of an array or object where Trino returns NULL, so those are tested for first
     (json_string keeps a string's quotes, so a string starting with '[' is not one).
     get_json_string requires a VARCHAR JSON string, so cast in case json_col is JSON-typed
     (e.g. an element produced by unnest_json_array/unnest_json_map on StarRocks).
     Still unlike Trino: a number with a decimal point or exponent is reformatted (10.0
     gives 10, 1e3 gives 1000), and the root path of the JSON text null gives 'null'. #}
  case
      when left(json_string(json_query(parse_json(cast({{ json_col }} as varchar)), {{ json_path }})), 1)
          not in ('[', '{')
          then get_json_string(cast({{ json_col }} as varchar), {{ json_path }})
  end
{% endmacro %}
