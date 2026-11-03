{#
    date_parse: `date_string` read as a timestamp with a Trino format string. Fails the
    query where a non-NULL string does not match the format.
    StarRocks evaluates every CASE, if and coalesce branch for a batch of rows that take
    different branches, so a strict parse in a branch also runs on the rows its guard
    skips. Pass that guard as `null_when`, a predicate with "{}" standing for the input
    (e.g. "{} = 'null'"): StarRocks does not check a value it matches, which leaves the
    caller's CASE to map it to NULL. With `strict=false` StarRocks gives NULL for any
    string that does not match, for a parse inside try_or_null (StarRocks has no try()).
    Trino and DuckDB ignore both.
#}
{% macro date_parse(date_string, format_string, strict=true, null_when=none) %}
  {{ return(adapter.dispatch('date_parse', 'open_learning')(date_string, format_string, strict, null_when)) }}
{% endmacro %}

{% macro trino__date_parse(date_string, format_string, strict=true, null_when=none) %}
  date_parse({{ date_string }}, {{ format_string }})
{% endmacro %}

{% macro duckdb__date_parse(date_string, format_string, strict=true, null_when=none) %}
  {# DuckDB uses strptime instead of date_parse #}
  {# Also need to convert format string from Trino format to DuckDB format #}
  {# Trino: %i for minutes, DuckDB: %M for minutes #}
  {# This is a simplified version - may need more format conversions #}
  strptime({{ date_string }}, replace(replace({{ format_string }}, '%i', '%M'), '%s', '%S'))
{% endmacro %}

{% macro default__date_parse(date_string, format_string, strict=true, null_when=none) %}
  {{ return(trino__date_parse(date_string, format_string, strict, null_when)) }}
{% endmacro %}

{#
    StarRocks: str_to_date returns NULL for a string that does not match the format, where
    Trino's date_parse raises, so the strict form raises where str_to_date is NULL (the rule
    the strict JSON macros follow). str_to_date is still more lenient than Trino: it accepts
    trailing text and a date with the format's time left off. The lambda binds the input so
    it is rendered once.
#}
{% macro starrocks__date_parse(date_string, format_string, strict=true, null_when=none) %}
  {%- if strict %}
  array_map(
      (date_value, parse_format) -> if(
          assert_true(
              date_value is null
              {%- if null_when is not none %}
              or ({{ null_when | replace('{}', 'date_value') }})
              {%- endif %}
              or str_to_date(date_value, parse_format) is not null
              , 'date string does not match its date_parse format'
          )
          , str_to_date(date_value, parse_format)
          , null
      )
      , [{{ date_string }}]
      , [{{ format_string }}]
  )[1]
  {%- else %}
  str_to_date({{ date_string }}, {{ format_string }})
  {%- endif %}
{% endmacro %}
