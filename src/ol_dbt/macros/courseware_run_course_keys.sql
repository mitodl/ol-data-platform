{#
    One row per distinct (platform, courserun_readable_id) in `runs`, with the
    course_key the run counts toward for cross-course support: the run's own
    course_fk; else the course_fk of another current run of the same course; else
    the platform and the course's readable id (e.g. residential, which dim_course
    does not hold).

    The middle step keeps reruns of one course together when only some of them carry
    a course_fk. dim_course_run does not version a run when only its course_fk
    changes, so a run first loaded before dim_course held its course keeps a null
    course_fk while later reruns get one. A run's own course_fk still wins, so a
    catalog mapping (an Emeritus run filed under its xPro course) is kept.
#}
{% macro courseware_run_course_keys(runs) -%}
    select
        runs.platform
        , runs.courserun_readable_id
        , coalesce(
            run_courses.course_fk
            , sibling_courses.course_fk
            , concat(runs.platform, '|', runs.course_readable_id)
        ) as course_key
    from (
        select distinct
            platform
            , courserun_readable_id
            , {{ courserun_course_readable_id('courserun_readable_id') }} as course_readable_id
        from {{ runs }}
        where courserun_readable_id is not null
    ) as runs
    left join (
        select
            platform
            , courserun_readable_id
            , max(course_fk) as course_fk
        from {{ ref('dim_course_run') }}
        where is_current
        group by platform, courserun_readable_id
    ) as run_courses
        on
            runs.platform = run_courses.platform
            and runs.courserun_readable_id = run_courses.courserun_readable_id
    left join (
        select
            platform
            , {{ courserun_course_readable_id('courserun_readable_id') }} as course_readable_id
            , max(course_fk) as course_fk
        from {{ ref('dim_course_run') }}
        where is_current
        group by platform, {{ courserun_course_readable_id('courserun_readable_id') }}
    ) as sibling_courses
        on
            runs.platform = sibling_courses.platform
            and runs.course_readable_id = sibling_courses.course_readable_id
{%- endmacro %}
