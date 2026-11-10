{#
    cast_date_to_iso8601: a date, or an ISO 8601 date string, as YYYY-MM-DD. Fails the
    query where a non-NULL value is neither.
    StarRocks evaluates every CASE, if and coalesce branch for a batch of rows that take
    different branches, so a call in a branch also runs on the rows its guard skips. Pass
    that guard as `null_when`, a predicate with "{}" standing for the input (e.g.
    "{} = 'null'"), as date_parse takes it. Trino and DuckDB ignore it.
#}
{% macro cast_date_to_iso8601(column_name, null_when=none) %}
  {{ return(adapter.dispatch('cast_date_to_iso8601', 'open_learning')(column_name, null_when)) }}
{% endmacro %}

{% macro trino__cast_date_to_iso8601(column_name, null_when=none) %}
  case
    when try_cast({{ column_name }} AS date) is not null -- Use try_cast to check if the column is already a date
       then to_iso8601(try_cast({{ column_name }} AS date))

    else to_iso8601(from_iso8601_date(try_cast({{ column_name }} AS varchar))) -- Convert to ISO 8601 from a string
end
{% endmacro %}

{% macro duckdb__cast_date_to_iso8601(column_name, null_when=none) %}
  case
    when try_cast({{ column_name }} AS date) is not null
       then strftime(try_cast({{ column_name }} AS date), '%Y-%m-%d')

    else strftime(strptime(try_cast({{ column_name }} AS varchar), '%Y-%m-%d'), '%Y-%m-%d')
end
{% endmacro %}

{% macro default__cast_date_to_iso8601(column_name, null_when=none) %}
  {# Default to Trino behavior for backward compatibility #}
  {{ return(trino__cast_date_to_iso8601(column_name, null_when)) }}
{% endmacro %}

{#
    StarRocks has no try_cast, and a failed cast or str_to_date returns NULL where Trino's
    from_iso8601_date raises, so this raises where both are NULL for a non-NULL value (the
    rule the strict JSON macros and date_parse follow). The engines still differ at the
    edges: Trino's from_iso8601_date reads reduced forms (2026, 2026-11, week and ordinal
    dates), which this rejects, and str_to_date accepts trailing text after the date,
    which Trino rejects. The lambda binds the input so it is rendered once.
#}
{% macro starrocks__cast_date_to_iso8601(column_name, null_when=none) %}
  array_map(
      (date_value) -> if(
          assert_true(
              date_value is null
              {%- if null_when is not none %}
              or ({{ null_when | replace('{}', 'date_value') }})
              {%- endif %}
              or cast(date_value as date) is not null
              or str_to_date(cast(date_value as varchar), '%Y-%m-%d') is not null
              , 'value is not a date or an ISO 8601 date string'
          )
          , date_format(
              coalesce(cast(date_value as date), str_to_date(cast(date_value as varchar), '%Y-%m-%d'))
              , '%Y-%m-%d'
          )
          , null
      )
      , [{{ column_name }}]
  )[1]
{% endmacro %}
