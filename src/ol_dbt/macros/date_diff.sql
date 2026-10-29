{% macro date_diff(unit, date1, date2) %}
  {{ return(adapter.dispatch('date_diff', 'open_learning')(unit, date1, date2)) }}
{% endmacro %}

{% macro trino__date_diff(unit, date1, date2) %}
  date_diff({{ unit }}, {{ date1 }}, {{ date2 }})
{% endmacro %}

{% macro duckdb__date_diff(unit, date1, date2) %}
  {# Whole units from date1 to date2, as in Trino. DuckDB's date_diff counts unit boundaries crossed instead. #}
  date_sub({{ unit }}, {{ date1 }}, {{ date2 }})
{% endmacro %}

{% macro default__date_diff(unit, date1, date2) %}
  {{ return(trino__date_diff(unit, date1, date2)) }}
{% endmacro %}

{% macro starrocks__date_diff(unit, date1, date2) %}
  {# StarRocks date_diff has reversed argument order compared to Trino, like DuckDB. Never use the 2-arg datediff(). #}
  date_diff({{ unit }}, {{ date2 }}, {{ date1 }})
{% endmacro %}
