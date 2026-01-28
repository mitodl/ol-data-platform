{% macro safe_parse_iso8601_date(varchar_field) %}
    CASE
        WHEN {{ varchar_field }} IS NULL THEN NULL
        WHEN {{ varchar_field }} = '' THEN NULL
        -- Handle format: YYYY-MM-DDTHH:MM:SS.sss
        WHEN LENGTH({{ varchar_field }}) >= 19 THEN
            DATE(
                FROM_ISO8601_TIMESTAMP(
                    SUBSTRING({{ varchar_field }}, 1, 19) || 'Z'
                )
            )
        -- Handle format: YYYY-MM-DD
        WHEN LENGTH({{ varchar_field }}) = 10 THEN
            DATE(FROM_ISO8601_DATE({{ varchar_field }}))
        ELSE NULL
    END
{% endmacro %}
