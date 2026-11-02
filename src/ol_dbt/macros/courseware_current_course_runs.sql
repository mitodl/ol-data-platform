{#
    Current course runs with course_key (courseware_course_key), the key that decides
    which runs are the same course, both when one course's runs cover another's
    prerequisites and when courses corroborate an inferred requirement
    (bridge_courserun_concept course_support): dim_course.course_readable_id, or, when
    course_fk is null (edxorg runs until edxorg courses load, and courses missing from
    dim_course), the run id less its last segment. It leaves out the platform:
    dim_course keys a course on (primary_platform, course_readable_id), so course_fk
    alone would let an edxorg and a mitxonline copy with the same readable id cover
    each other, or count as two courses of support.
#}
{% macro courseware_current_course_runs() -%}
    select
        dim_course_run.courserun_pk
        , dim_course_run.platform
        , dim_course_run.courserun_readable_id
        , {{ courseware_course_key('dim_course_run.courserun_readable_id', 'courses.course_readable_id') }}
        as course_key
    from {{ ref('dim_course_run') }} as dim_course_run
    left join (
        select * from {{ ref('dim_course') }}
        where is_current
    ) as courses
        on dim_course_run.course_fk = courses.course_pk
    where dim_course_run.is_current
{%- endmacro %}
