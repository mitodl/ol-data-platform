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


{#
    format_datetime: Format a date/timestamp using a Java-style (Trino) or strftime-style (DuckDB) pattern.
    For cross-db use, map from Java DateTime format (Trino) to strftime format (DuckDB).
    Common mappings: 'yyyyMMdd' -> '%Y%m%d', 'EEEE' -> '%A', 'MMMM' -> '%B'
#}
{% macro format_datetime(datetime_expr, java_format) -%}
    {{ adapter.dispatch('format_datetime', 'open_learning')(datetime_expr, java_format) }}
{%- endmacro %}

{% macro default__format_datetime(datetime_expr, java_format) -%}
    {# Trino: native format_datetime with Java DateTime format #}
    format_datetime({{ datetime_expr }}, '{{ java_format }}')
{%- endmacro %}

{% macro duckdb__format_datetime(datetime_expr, java_format) -%}
    {# DuckDB: strftime with %-style format. Convert common Java patterns. #}
    {% set strftime_format = java_format
        | replace('yyyy', '%Y')
        | replace('MMMM', '%B')
        | replace('MMM', '%b')
        | replace('MM', '%m')
        | replace('dd', '%d')
        | replace('EEEE', '%A')
        | replace('EEE', '%a')
        | replace('HH', '%H')
        | replace('mm', '%M')
        | replace('ss', '%S')
    %}
    strftime({{ datetime_expr }}, '{{ strftime_format }}')
{%- endmacro %}


{#
    date_format: Format a date/timestamp using a MySQL-style format string (Trino date_format).
    DuckDB uses strftime with the same %-style format strings.
#}
{% macro date_format(datetime_expr, format_string) -%}
    {{ adapter.dispatch('date_format', 'open_learning')(datetime_expr, format_string) }}
{%- endmacro %}

{% macro default__date_format(datetime_expr, format_string) -%}
    {# Trino: date_format with MySQL-style format string #}
    date_format({{ datetime_expr }}, {{ format_string }})
{%- endmacro %}

{% macro duckdb__date_format(datetime_expr, format_string) -%}
    {# DuckDB: strftime uses same %-style format strings as Trino date_format #}
    strftime({{ datetime_expr }}, {{ format_string }})
{%- endmacro %}


{#
    day_of_week: ISO day of week (1=Monday, 7=Sunday) consistent with Trino day_of_week().
#}
{% macro day_of_week(date_expr) -%}
    {{ adapter.dispatch('day_of_week', 'open_learning')(date_expr) }}
{%- endmacro %}

{% macro default__day_of_week(date_expr) -%}
    {# Trino: day_of_week returns 1=Mon, 7=Sun #}
    day_of_week({{ date_expr }})
{%- endmacro %}

{% macro duckdb__day_of_week(date_expr) -%}
    {# DuckDB: isodow returns 1=Mon, 7=Sun (same as Trino) #}
    isodow({{ date_expr }})
{%- endmacro %}


{#
    iso8601_to_date_key: Convert an ISO8601 varchar date/datetime field to an integer YYYYMMDD date key.
    Handles both 'YYYY-MM-DD' (10 chars) and 'YYYY-MM-DDTHH:MM:SS...' (>=19 chars) formats.
    Returns NULL if input is NULL.
#}
{% macro iso8601_to_date_key(varchar_field) -%}
    {{ return(adapter.dispatch('iso8601_to_date_key', 'open_learning')(varchar_field)) }}
{%- endmacro %}

{% macro default__iso8601_to_date_key(varchar_field) -%}
    {# Trino: date_parse + date_format #}
    CASE
        WHEN {{ varchar_field }} IS NULL THEN NULL
        WHEN LENGTH({{ varchar_field }}) = 10 THEN
            CAST(date_format(date_parse({{ varchar_field }}, '%Y-%m-%d'), '%Y%m%d') AS INTEGER)
        WHEN LENGTH({{ varchar_field }}) >= 19 THEN
            CAST(date_format(date_parse(SUBSTR({{ varchar_field }}, 1, 19), '%Y-%m-%dT%H:%i:%s'), '%Y%m%d') AS INTEGER)
        ELSE NULL
    END
{%- endmacro %}

{% macro duckdb__iso8601_to_date_key(varchar_field) -%}
    {# DuckDB: strptime + strftime #}
    CASE
        WHEN {{ varchar_field }} IS NULL THEN NULL
        WHEN LENGTH({{ varchar_field }}) = 10 THEN
            CAST(strftime(strptime({{ varchar_field }}, '%Y-%m-%d'), '%Y%m%d') AS INTEGER)
        WHEN LENGTH({{ varchar_field }}) >= 19 THEN
            CAST(strftime(strptime(SUBSTR({{ varchar_field }}, 1, 19), '%Y-%m-%dT%H:%M:%S'), '%Y%m%d') AS INTEGER)
        ELSE NULL
    END
{%- endmacro %}

{% macro is_courserun_current(start_on_timestamp_str, end_on_timestamp_str) -%}
    {{ adapter.dispatch('is_courserun_current', 'open_learning')(start_on_timestamp_str, end_on_timestamp_str) }}
{%- endmacro %}

{% macro default__is_courserun_current(start_on_timestamp_str, end_on_timestamp_str) -%}
   {# Trino: native support #}
    case
        when
            cast(from_iso8601_timestamp({{ start_on_timestamp_str }}) as date) <= current_date
            and (
                {{ end_on_timestamp_str }} is null
                or cast(from_iso8601_timestamp({{ end_on_timestamp_str }}) as date) >= current_date
            )
        then true
        else false
    end
{%- endmacro %}

{% macro duckdb__is_courserun_current(start_on_timestamp_str, end_on_timestamp_str) -%}
    case
        when
            cast({{ start_on_timestamp_str }} as date) <= current_date
            and (
                {{ end_on_timestamp_str }} is null
                or cast({{ end_on_timestamp_str }} as date) >= current_date
            )
        then true
        else false
    end
{%- endmacro %}
