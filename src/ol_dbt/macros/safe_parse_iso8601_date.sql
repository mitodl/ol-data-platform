{% macro safe_parse_iso8601_date(varchar_field) %}
    CASE
        WHEN {{ varchar_field }} IS NULL THEN NULL
        WHEN {{ varchar_field }} = '' THEN NULL
        -- Handle format: YYYY-MM-DDTHH:MM:SS.sss or YYYY-MM-DDTHH:MM:SS
        WHEN LENGTH({{ varchar_field }}) >= 19 THEN
            CAST({{ date_parse("SUBSTRING(" ~ varchar_field ~ ", 1, 19)", "'%Y-%m-%dT%H:%i:%s'") }} AS DATE)
        -- Handle format: YYYY-MM-DD
        WHEN LENGTH({{ varchar_field }}) = 10 THEN
            CAST({{ date_parse(varchar_field, "'%Y-%m-%d'") }} AS DATE)
        ELSE NULL
    END
{% endmacro %}
