{% macro translate_course_id_to_platform(course_id) %}
        case
            when {{ course_id }} like 'MITxT%' then '{{ var("mitxonline") }}'
            when {{ course_id }} like 'xPRO%' then '{{ var("mitxpro") }}'
            when {{ course_id }} is null then null
            else '{{ var("edxorg") }}'
        end
{% endmacro %}
