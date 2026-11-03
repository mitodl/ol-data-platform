{#
    The key that decides which course runs are the same course, across platforms:
    course_readable_id (dim_course's, when the run has a course), or else the run id
    less its last segment (courserun_course_readable_id). An {org}/{course} key is
    rewritten as course-v1:{org}+{course}, because dim_course_run holds every edxorg
    run in the {org}/{course}/{run} format, so without it an edxorg copy (MITxT/14.100x)
    would never match its mitxonline copy (course-v1:MITxT+14.100x) or an extracted
    run id in the course-v1 format.
#}
{% macro courseware_course_key(courserun_readable_id, course_readable_id=none) -%}
    {%- set key = courserun_course_readable_id(courserun_readable_id) -%}
    {%- if course_readable_id is not none -%}
        {%- set key = "coalesce(" ~ course_readable_id ~ ", " ~ key ~ ")" -%}
    {%- endif -%}
    case
        when {{ regexp_like(key, "'^[^/]+/[^/]+$'") }}
            then concat('course-v1:', replace({{ key }}, '/', '+'))
        else {{ key }}
    end
{%- endmacro %}
