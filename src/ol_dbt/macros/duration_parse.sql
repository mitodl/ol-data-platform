{#
  A course duration written as text, as the MITPE and SEE feeds write it ("8 weeks",
  "3-5 days", "6 to 8 Weeks", "7 half days", "2 meses/curso"): its first number, the
  number after a range separator, and its unit.

  A model computes the text parts once in a CTE and reads them in the next:
    duration_parse_parts(text_expr)  select-list columns duration_min_value and
                                     duration_max_value (text) and duration_unit_word
    duration_min(prefix)             decimal(8, 2), the first number
    duration_max(prefix)             decimal(8, 2), the number after a range separator,
                                     else the first
    duration_unit(prefix)            hour, half_day, day, week, month or semester; null
                                     for any other word
  The last three read the parts columns, prefixed with e.g. 'offerings.', and leave
  the alias to the model, so column lineage sees it.
  This keeps the feeds' units. MIT Learn's reading of the same text, in weeks, is
  learn_duration_weeks.
#}

{% macro duration_parse_parts(text_expr) -%}
    {#- "7 half days" and "5 Half-Days" are one unit, so join them before taking the
        word after the number -#}
    {%- set normalized -%}
        replace(replace(lower(trim({{ text_expr }})), 'half days', 'half-days'), 'half day', 'half-day')
    {%- endset -%}
    {{ regexp_extract_or_null(normalized, "'^(\\d+(?:\\.\\d+)?)'", 1) }} as duration_min_value
    , {{ regexp_extract_or_null(normalized, "'^\\d+(?:\\.\\d+)?\\s*(?:-|–|to)\\s*(\\d+(?:\\.\\d+)?)'", 1) }}
        as duration_max_value
    {#- the first word after the number or range (and an optional hyphen, "8-week"), up
        to a space or punctuation ("3 mes/curso", "3 days, online", "2 days in
        person+3 live webinars") -#}
    , {{ regexp_extract_or_null(
        normalized,
        "'^\\d+(?:\\.\\d+)?(?:\\s*(?:-|–|to)\\s*\\d+(?:\\.\\d+)?)?\\s*-?\\s*([^\\s/+,.;:()]+)'",
        1
    ) }} as duration_unit_word
{%- endmacro %}

{% macro duration_min(prefix='') -%}
    {{ try_cast(prefix ~ 'duration_min_value', 'decimal(8, 2)') }}
{%- endmacro %}

{% macro duration_max(prefix='') -%}
    {{ try_cast('coalesce(' ~ prefix ~ 'duration_max_value, ' ~ prefix ~ 'duration_min_value)', 'decimal(8, 2)') }}
{%- endmacro %}

{% macro duration_unit(prefix='') -%}
    case
        when {{ prefix }}duration_min_value is null then null
        when {{ prefix }}duration_unit_word in ('hour', 'hours', 'hora', 'horas') then 'hour'
        when {{ prefix }}duration_unit_word in ('half-day', 'half-days') then 'half_day'
        when {{ prefix }}duration_unit_word in ('day', 'days', 'día', 'días', 'jour', 'jours') then 'day'
        when {{ prefix }}duration_unit_word in (
            'week', 'weeks', 'semana', 'semanas', 'semaine', 'semaines', 'settimana', 'settimane'
        ) then 'week'
        when {{ prefix }}duration_unit_word in ('month', 'months', 'mes', 'meses', 'mois', 'mesi') then 'month'
        when {{ prefix }}duration_unit_word in ('semester', 'semesters', 'semestre', 'semestres') then 'semester'
    end
{%- endmacro %}
