{% macro is_courserun_current(courserun_start_on, courserun_end_on) %}
    case
        when
            date(from_iso8601_timestamp({{ courserun_start_on }})) <= current_date
            and
            (
                {{ courserun_end_on }} is null
                or date(from_iso8601_timestamp({{ courserun_end_on }})) >= current_date
            )
        then true
        else false
    end
{% endmacro %}
