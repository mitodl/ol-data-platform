{#
  format_timestamp_as_iso8601: an instant as an ISO 8601 string in UTC.

  fractional_digits, 3 or 6, is how many fractional digits the StarRocks and DuckDB
  bodies write. Trino ignores it: to_iso8601 writes the precision of the value's type,
  e.g. 6 for a timestamp column of an Iceberg table and 3 for one built in the query
  with cast(... as timestamp) or date_parse. Pass what Trino writes for the expression.
  StarRocks' DATETIME and DuckDB's TIMESTAMPTZ carry no precision and hold at most 6.
#}
{% macro format_timestamp_as_iso8601(timestamp_expr, fractional_digits=6) %}
  {% if fractional_digits not in (3, 6) %}
    {{ exceptions.raise_compiler_error("format_timestamp_as_iso8601: fractional_digits must be 3 or 6, got " ~ fractional_digits) }}
  {% endif %}
  {{ return(adapter.dispatch('format_timestamp_as_iso8601', 'open_learning')(timestamp_expr, fractional_digits)) }}
{% endmacro %}

{% macro trino__format_timestamp_as_iso8601(timestamp_expr, fractional_digits=6) %}
  {# timestamp_expr is assumed to already be a valid timestamp/timestamp with time zone
     expression (e.g. the output of from_iso8601_timestamp_nanos) -- format it directly rather
     than routing it through cast_timestamp_to_iso8601's generic try_cast(... AS timestamp),
     which would downcast a timestamp(9) with time zone value to the default timestamp(3)
     (see https://trino.io/docs/current/language/types.html), rounding away sub-millisecond
     precision and dropping the zone. #}
  to_iso8601({{ timestamp_expr }})
{% endmacro %}

{% macro default__format_timestamp_as_iso8601(timestamp_expr, fractional_digits=6) %}
  {# Default to Trino behavior for backward compatibility #}
  {{ return(trino__format_timestamp_as_iso8601(timestamp_expr, fractional_digits)) }}
{% endmacro %}

{% macro duckdb__format_timestamp_as_iso8601(timestamp_expr, fractional_digits=6) %}
  {# DuckDB formats a TIMESTAMPTZ in the connection's session time zone, not UTC, and the
     dev/dev_local profiles don't pin one -- normalize to UTC first so the literal Z suffix
     below is actually correct instead of a session-local wall-clock time mislabeled as UTC.
     %g is milliseconds and %f microseconds. #}
  {%- set fraction = {3: '%g', 6: '%f'}[fractional_digits] -%}
  strftime(({{ timestamp_expr }}) at time zone 'UTC', '%Y-%m-%dT%H:%M:%S.{{ fraction }}Z')
{% endmacro %}

{% macro starrocks__format_timestamp_as_iso8601(timestamp_expr, fractional_digits=6) %}
  {# %f is always six digits; cut the string after the digits Trino would write. #}
  concat(
    substr(date_format({{ timestamp_expr }}, '%Y-%m-%dT%H:%i:%s.%f'), 1, {{ 20 + fractional_digits }}), 'Z'
  )
{% endmacro %}
