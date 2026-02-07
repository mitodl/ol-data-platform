{% macro cast_date_to_iso8601(column_name) %}
  {{ return(adapter.dispatch('cast_date_to_iso8601', 'open_learning')(column_name)) }}
{% endmacro %}

{% macro trino__cast_date_to_iso8601(column_name) %}
  case
    when try_cast({{ column_name }} AS date) is not null -- Use try_cast to check if the column is already a date
       then to_iso8601(try_cast({{ column_name }} AS date))

    else to_iso8601(from_iso8601_date(try_cast({{ column_name }} AS varchar))) -- Convert to ISO 8601 from a string
end
{% endmacro %}

{% macro duckdb__cast_date_to_iso8601(column_name) %}
  case
    when try_cast({{ column_name }} AS date) is not null
       then strftime(try_cast({{ column_name }} AS date), '%Y-%m-%d')
    
    else strftime(strptime(try_cast({{ column_name }} AS varchar), '%Y-%m-%d'), '%Y-%m-%d')
end
{% endmacro %}

{% macro default__cast_date_to_iso8601(column_name) %}
  {# Default to Trino behavior for backward compatibility #}
  {{ return(trino__cast_date_to_iso8601(column_name)) }}
{% endmacro %}
