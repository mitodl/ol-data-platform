{% macro learn_duration_weeks(duration_expr, bound) %}
  {#
    Lower or upper bound, in weeks, of a free-text duration such as "6 weeks",
    "2-3 days" or "3 Months", as MIT Learn's parse_resource_duration computed it
    (learning_resources/etl/utils.py). NULL when the text has no leading number or
    no recognized unit.

    bound: 'min' or 'max'. A range's upper number only counts after a separator, so
    "12 weeks" is not read as the range 1-2; with no upper number, max = min.

    Days count as working days (5 per week, at least 1 week) and months as 4 weeks.
    Hour units fall through as weeks, as they did in MIT Learn.
  #}
  {%- if bound not in ('min', 'max') -%}
    {{ exceptions.raise_compiler_error("learn_duration_weeks: bound must be 'min' or 'max', got " ~ bound) }}
  {%- endif -%}
  {%- set normalized = "lower(trim(" ~ duration_expr ~ "))" -%}
  {%- set min_raw = regexp_extract_or_null(normalized, "'^(\\d+)'", 1) -%}
  {%- set max_raw = regexp_extract_or_null(normalized, "'^\\d+(?:\\s*(?:to|-)+\\s*|\\s+)(\\d+)'", 1) -%}
  {%- set number_raw = min_raw if bound == 'min' else "coalesce(" ~ max_raw ~ ", " ~ min_raw ~ ")" -%}
  {#- The first English unit anywhere in the string, else the first Spanish, French or
      Italian one. As in MIT Learn's pattern, only the last alternative of each needs a
      separator after it, so "mes" doesn't match inside "semesters". -#}
  {%- set english_unit = regexp_extract_or_null(
      "lower(" ~ duration_expr ~ ")",
      "'half-days|half-day|hours|hour|days|day|weeks|week|months|month(\\s|/|$)'"
  ) -%}
  {%- set other_unit = regexp_extract_or_null(
      "lower(" ~ duration_expr ~ ")",
      "'horas|hora|días|jours|día|jour|semanas|semaines|settimanes|semana|semaine|settimane|meses|mois|mesi|mes(\\s|/|$)'"
  ) -%}
  case
      when {{ min_raw }} is null then null
      when {{ english_unit }} like '%day%'
          then greatest(cast(ceil(cast({{ number_raw }} as double) / 5) as integer), 1)
      when {{ english_unit }} like '%month%' then cast({{ number_raw }} as integer) * 4
      when {{ english_unit }} is not null then cast({{ number_raw }} as integer)
      when {{ other_unit }} in ('días', 'jours', 'día', 'jour')
          then greatest(cast(ceil(cast({{ number_raw }} as double) / 5) as integer), 1)
      -- "mes" can match with its trailing separator ("mes/"); MIT Learn raised
      -- KeyError on that, and it is a month
      when trim(replace({{ other_unit }}, '/', '')) in ('meses', 'mois', 'mesi', 'mes')
          then cast({{ number_raw }} as integer) * 4
      when {{ other_unit }} is not null then cast({{ number_raw }} as integer)
  end
{% endmacro %}
