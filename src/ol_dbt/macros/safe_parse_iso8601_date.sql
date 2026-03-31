{% macro safe_parse_iso8601_date(varchar_field) %}
    {{ adapter.dispatch('safe_parse_iso8601_date', 'open_learning')(varchar_field) }}
{% endmacro %}

{% macro default__safe_parse_iso8601_date(varchar_field) %}
    {# Trino: date_parse is a native function; wrap in try_cast for safety #}
    try_cast(
        CASE
            WHEN {{ varchar_field }} IS NULL THEN NULL
            WHEN {{ varchar_field }} = '' THEN NULL
            -- Handle format: YYYY-MM-DDTHH:MM:SS.sss or YYYY-MM-DDTHH:MM:SS
            WHEN LENGTH({{ varchar_field }}) >= 19 THEN
                date_parse(SUBSTRING({{ varchar_field }}, 1, 19), '%Y-%m-%dT%H:%i:%s')
            -- Handle format: YYYY-MM-DD
            WHEN LENGTH({{ varchar_field }}) = 10 THEN
                date_parse({{ varchar_field }}, '%Y-%m-%d')
            ELSE NULL
        END
        AS DATE
    )
{% endmacro %}

{% macro duckdb__safe_parse_iso8601_date(varchar_field) %}
    {# DuckDB: try_strptime returns NULL on parse failure, avoiding hard errors #}
    CASE
        WHEN {{ varchar_field }} IS NULL THEN NULL
        WHEN {{ varchar_field }} = '' THEN NULL
        -- Handle format: YYYY-MM-DDTHH:MM:SS.sss or YYYY-MM-DDTHH:MM:SS
        WHEN LENGTH({{ varchar_field }}) >= 19 THEN
            CAST(try_strptime(SUBSTRING({{ varchar_field }}, 1, 19), '%Y-%m-%dT%H:%M:%S') AS DATE)
        -- Handle format: YYYY-MM-DD
        WHEN LENGTH({{ varchar_field }}) = 10 THEN
            CAST(try_strptime({{ varchar_field }}, '%Y-%m-%d') AS DATE)
        ELSE NULL
    END
{% endmacro %}
