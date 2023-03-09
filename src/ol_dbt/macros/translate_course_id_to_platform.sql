{% macro translate_course_id_to_platform(course_id) %}
        case
            when {{ course_id }} like 'MITxT%' then '{{ var("mitxonline") }}'
            ---- only course_id starts with xPRO are from xPro open edx platform
            when {{ course_id }} like 'xPRO%' then '{{ var("mitxpro") }}'
            --- Some runs from course - VJx Visualizing Japan (1850s-1930s) that run on edx don't start with 'MITx/`
            --- e.g. VJx/VJx_S/3T2015, VJx/VJx/3T2014, VJx/VJx_2/3T2016
            when {{ course_id }} like 'MITx/%' or {{ course_id }} like 'VJx%' then '{{ var("edxorg") }}'
            else null
        end
{% endmacro %}
