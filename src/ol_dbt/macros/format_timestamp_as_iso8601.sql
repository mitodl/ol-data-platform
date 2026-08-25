{% macro format_timestamp_as_iso8601(timestamp_expr) %}
  {{ return(adapter.dispatch('format_timestamp_as_iso8601', 'open_learning')(timestamp_expr)) }}
{% endmacro %}

{% macro trino__format_timestamp_as_iso8601(timestamp_expr) %}
  {# timestamp_expr is assumed to already be a valid timestamp/timestamp with time zone
     expression (e.g. the output of from_iso8601_timestamp_nanos) -- format it directly rather
     than routing it through cast_timestamp_to_iso8601's generic try_cast(... AS timestamp),
     which would downcast a timestamp(9) with time zone value to the default timestamp(3)
     (see https://trino.io/docs/current/language/types.html), rounding away sub-millisecond
     precision and dropping the zone. #}
  to_iso8601({{ timestamp_expr }})
{% endmacro %}

{% macro default__format_timestamp_as_iso8601(timestamp_expr) %}
  {# Default to Trino behavior for backward compatibility #}
  {{ return(trino__format_timestamp_as_iso8601(timestamp_expr)) }}
{% endmacro %}

{% macro duckdb__format_timestamp_as_iso8601(timestamp_expr) %}
  {# Format the already-typed timestamp/timestamptz directly -- no intermediate cast #}
  strftime({{ timestamp_expr }}, '%Y-%m-%dT%H:%M:%S.%fZ')
{% endmacro %}

{% macro starrocks__format_timestamp_as_iso8601(timestamp_expr) %}
  {# Not exercised by any current caller against a live StarRocks target -- verify before relying on it. #}
  date_format({{ timestamp_expr }}, '%Y-%m-%dT%H:%i:%s.%fZ')
{% endmacro %}
