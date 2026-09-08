{% macro regexp_replace_with_backreferences(column_name, pattern, trino_replacement) %}
  {{ return(adapter.dispatch('regexp_replace_with_backreferences', 'open_learning')(column_name, pattern, trino_replacement)) }}
{% endmacro %}

{% macro trino__regexp_replace_with_backreferences(column_name, pattern, trino_replacement) %}
  regexp_replace({{ column_name }}, {{ pattern }}, {{ trino_replacement }})
{% endmacro %}

{% macro default__regexp_replace_with_backreferences(column_name, pattern, trino_replacement) %}
  {# Default to Trino behavior for backward compatibility #}
  {{ return(trino__regexp_replace_with_backreferences(column_name, pattern, trino_replacement)) }}
{% endmacro %}

{% macro duckdb__regexp_replace_with_backreferences(column_name, pattern, trino_replacement) %}
  {# DuckDB's regexp_replace uses the RE2 engine (https://duckdb.org/docs/current/sql/functions/regular_expressions),
     whose replacement-string backreferences are \1, \2, ... rather than Trino's $1, $2, ...;
     translate the caller's Trino-style replacement string instead of asking every caller to
     write it twice. #}
  regexp_replace({{ column_name }}, {{ pattern }}, {{ trino_replacement | replace('$', '\\') }})
{% endmacro %}

{% macro starrocks__regexp_replace_with_backreferences(column_name, pattern, trino_replacement) %}
  {# StarRocks' regexp_replace follows the same \1, \2, ... backreference convention as DuckDB.
     No current caller runs this against a live StarRocks target -- verify before relying on it. #}
  regexp_replace({{ column_name }}, {{ pattern }}, {{ trino_replacement | replace('$', '\\') }})
{% endmacro %}
