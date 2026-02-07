{% macro array_join(array_col, delimiter) %}
  {{ return(adapter.dispatch('array_join', 'open_learning')(array_col, delimiter)) }}
{% endmacro %}

{% macro trino__array_join(array_col, delimiter) %}
  array_join({{ array_col }}, {{ delimiter }})
{% endmacro %}

{% macro duckdb__array_join(array_col, delimiter) %}
  list_string_agg({{ array_col }}, {{ delimiter }})
{% endmacro %}

{% macro default__array_join(array_col, delimiter) %}
  {{ return(trino__array_join(array_col, delimiter)) }}
{% endmacro %}
