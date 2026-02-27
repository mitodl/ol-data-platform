{% macro date_diff(unit, date1, date2) %}
  {{ return(adapter.dispatch('date_diff', 'open_learning')(unit, date1, date2)) }}
{% endmacro %}

{% macro trino__date_diff(unit, date1, date2) %}
  date_diff({{ unit }}, {{ date1 }}, {{ date2 }})
{% endmacro %}

{% macro duckdb__date_diff(unit, date1, date2) %}
  {# DuckDB date_diff has reversed argument order compared to Trino #}
  date_diff({{ unit }}, {{ date2 }}, {{ date1 }})
{% endmacro %}

{% macro default__date_diff(unit, date1, date2) %}
  {{ return(trino__date_diff(unit, date1, date2)) }}
{% endmacro %}
