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

{% macro starrocks__cast_timestamp_to_iso8601(column_name) %}
  {# StarRocks has no try_cast; a failed cast returns NULL, so cast doubles as the "is it already a timestamp" check.
     %f emits 6-digit microseconds vs Trino's 3-digit milliseconds -- cosmetic unless the strings are joined
     across engines. #}
  case
       when cast({{ column_name }} as datetime) is not null
          then date_format(cast({{ column_name }} as datetime), '%Y-%m-%dT%H:%i:%s.%fZ')
       else date_format(str_to_date(replace(cast({{ column_name }} as varchar), 'T', ' '), '%Y-%m-%d %H:%i:%s'), '%Y-%m-%dT%H:%i:%s.%fZ')
   end
{% endmacro %}
