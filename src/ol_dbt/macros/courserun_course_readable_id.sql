{#
    The readable id of the course a run belongs to: the run id less its last segment,
    as dim_course_run derives the id it joins dim_course on. For a run with no
    course_fk (residential runs, which dim_course does not hold) this keys reruns of
    one course together. extract_course_readable_id is not used because it stops at
    an underscore (MITx+15.390.1x_KOR becomes MITx+15.390.1x) and returns 'course-v1:'
    for ids in neither format.
#}
{% macro courserun_course_readable_id(courserun_readable_id) -%}
    case
        when {{ courserun_readable_id }} like 'course-v1:%'
            then substring(
                {{ courserun_readable_id }}
                , 1
                , length({{ courserun_readable_id }}) - strpos(reverse({{ courserun_readable_id }}), '+')
            )
        when {{ regexp_like(courserun_readable_id, "'^[^/]+/[^/]+/[^/]+'") }}
            then substring(
                {{ courserun_readable_id }}
                , 1
                , length({{ courserun_readable_id }}) - strpos(reverse({{ courserun_readable_id }}), '/')
            )
        else {{ courserun_readable_id }}
    end
{%- endmacro %}
