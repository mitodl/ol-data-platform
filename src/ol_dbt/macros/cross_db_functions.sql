{#
    Cross-database compatibility macros for Trino, DuckDB, and StarRocks.

    These macros provide a unified interface for common SQL functions that have
    different implementations across database engines.
#}

{% macro from_iso8601_timestamp(timestamp_str) -%}
    {{ adapter.dispatch('from_iso8601_timestamp', 'open_learning')(timestamp_str) }}
{%- endmacro %}

{% macro default__from_iso8601_timestamp(timestamp_str) -%}
    {# Trino: native support #}
    from_iso8601_timestamp({{ timestamp_str }})
{%- endmacro %}

{% macro duckdb__from_iso8601_timestamp(timestamp_str) -%}
    {# DuckDB: use strptime or cast #}
    cast({{ timestamp_str }} as timestamp)
{%- endmacro %}

{% macro starrocks__from_iso8601_timestamp(timestamp_str) -%}
    {# StarRocks: use str_to_date or cast #}
    cast({{ timestamp_str }} as datetime)
{%- endmacro %}


{% macro array_join(array_expr, delimiter, null_replacement='') -%}
    {{ adapter.dispatch('array_join', 'open_learning')(array_expr, delimiter, null_replacement) }}
{%- endmacro %}

{% macro default__array_join(array_expr, delimiter, null_replacement='') -%}
    {# Trino: native support #}
    {% if null_replacement %}
        array_join({{ array_expr }}, '{{ delimiter }}', '{{ null_replacement }}')
    {% else %}
        array_join({{ array_expr }}, '{{ delimiter }}')
    {% endif %}
{%- endmacro %}

{% macro duckdb__array_join(array_expr, delimiter, null_replacement='') -%}
    {# DuckDB: use array_to_string or list_aggr #}
    array_to_string({{ array_expr }}, '{{ delimiter }}')
{%- endmacro %}

{% macro starrocks__array_join(array_expr, delimiter, null_replacement='') -%}
    {# StarRocks: array_join with different signature #}
    array_join({{ array_expr }}, '{{ delimiter }}')
{%- endmacro %}


{% macro regexp_like(string_expr, pattern) -%}
    {{ adapter.dispatch('regexp_like', 'open_learning')(string_expr, pattern) }}
{%- endmacro %}

{% macro default__regexp_like(string_expr, pattern) -%}
    {# Trino: native support #}
    regexp_like({{ string_expr }}, {{ pattern }})
{%- endmacro %}

{% macro duckdb__regexp_like(string_expr, pattern) -%}
    {# DuckDB: use regexp_matches #}
    regexp_matches({{ string_expr }}, {{ pattern }})
{%- endmacro %}

{% macro starrocks__regexp_like(string_expr, pattern) -%}
    {# StarRocks: regexp #}
    {{ string_expr }} regexp {{ pattern }}
{%- endmacro %}


{% macro element_at_array(array_expr, index) -%}
    {{ adapter.dispatch('element_at_array', 'open_learning')(array_expr, index) }}
{%- endmacro %}

{% macro default__element_at_array(array_expr, index) -%}
    {# Trino: element_at with 1-based indexing #}
    element_at({{ array_expr }}, {{ index }})
{%- endmacro %}

{% macro duckdb__element_at_array(array_expr, index) -%}
    {# DuckDB: array subscript with 1-based indexing (list_element also works) #}
    ({{ array_expr }})[{{ index }}]
{%- endmacro %}

{% macro starrocks__element_at_array(array_expr, index) -%}
    {# StarRocks: array subscript with 1-based indexing #}
    {{ array_expr }}[{{ index }}]
{%- endmacro %}

{% macro is_courserun_current(courserun_start_on, courserun_end_on) -%}
    {{ adapter.dispatch('is_courserun_current', 'open_learning')(courserun_start_on, courserun_end_on) }}
{%- endmacro %}

{% macro default__is_courserun_current(courserun_start_on, courserun_end_on) -%}
   {# Trino: native support #}
    case
        when
            cast(from_iso8601_timestamp({{ courserun_start_on }}) as date) <= current_date
            and (
                {{ courserun_end_on }} is null
                or cast(from_iso8601_timestamp({{ courserun_end_on }}) as date) >= current_date
            )
        then true
        else false
    end
{%- endmacro %}

{% macro duckdb__is_courserun_current(courserun_start_on, courserun_end_on) -%}
    {# DuckDB: Casting to date for compatibility #}
    case
        when
            cast(strptime({{ courserun_start_on }}, '%Y-%m-%dT%H:%M:%S') as date) <= current_date
            and (
                {{ courserun_end_on }} is null
                or cast(strptime({{ courserun_end_on }}, '%Y-%m-%dT%H:%M:%S') as date) >= current_date
            )
        then true
        else false
    end
{%- endmacro %}

{% macro starrocks__is_courserun_current(courserun_start_on, courserun_end_on) -%}
    {# StarRocks: Casting to date for comparisons #}
    case
        when
            cast({{ courserun_start_on }} as date) <= current_date
            and (
                {{ courserun_end_on }} is null
                or cast({{ courserun_end_on }} as date) >= current_date
            )
        then true
        else false
    end
{%- endmacro %}
