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
  {# StarRocks: json_query gives the value at the path; an array or object there gives NULL
     on Trino, so those are tested for first (json_string keeps a string's quotes, so a
     string starting with '[' is not one), and the cast to varchar unquotes a string and
     gives NULL for a JSON null.
     cast(json_col as json) parses a varchar and leaves a JSON value (an unnest_json_array
     element) as it is. Going through varchar instead would unquote a JSON string element
     and parse its text again, so "[1,2]" would read as an array.
     Still unlike Trino: a number with a decimal point, exponent or sign is reformatted
     (10.0 gives 10, 1e3 gives 1000, -0 gives 0), and the root path of the JSON text null
     gives 'null'. #}
  case
      when left(json_string(json_query(cast({{ json_col }} as json), {{ json_path }})), 1)
          not in ('[', '{')
          then cast(json_query(cast({{ json_col }} as json), {{ json_path }}) as varchar)
  end
{% endmacro %}
