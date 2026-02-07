{% macro cast_timestamp_to_iso8601(column_name) %}
  {{ return(adapter.dispatch('cast_timestamp_to_iso8601', 'open_learning')(column_name)) }}
{% endmacro %}

{% macro trino__cast_timestamp_to_iso8601(column_name) %}
  case
       -- If the column is already a timestamp, convert to ISO 8601
        when try_cast({{ column_name }} AS timestamp) is not null
           then to_iso8601(try_cast({{ column_name }} AS timestamp))
        -- otherwise, convert to ISO 8601 from a string
        else to_iso8601(from_iso8601_timestamp(try_cast({{ column_name }} AS varchar)))
   end
{% endmacro %}

{% macro duckdb__cast_timestamp_to_iso8601(column_name) %}
  case
       -- If the column is already a timestamp, convert to ISO 8601
        when try_cast({{ column_name }} AS timestamp) is not null
           then strftime(try_cast({{ column_name }} AS timestamp), '%Y-%m-%dT%H:%M:%S.%fZ')
        -- otherwise, parse from string and convert to ISO 8601
        else strftime(strptime(try_cast({{ column_name }} AS varchar), '%Y-%m-%d %H:%M:%S'), '%Y-%m-%dT%H:%M:%S.%fZ')
   end
{% endmacro %}

{% macro default__cast_timestamp_to_iso8601(column_name) %}
  {# Default to Trino behavior for backward compatibility #}
  {{ return(trino__cast_timestamp_to_iso8601(column_name)) }}
{% endmacro %}
