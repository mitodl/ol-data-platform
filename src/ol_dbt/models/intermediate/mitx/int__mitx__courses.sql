with mitxonline_courses as (
    select
        course_id as mitxonline_course_id
        , course_title
        , course_is_live
        , course_readable_id
        , course_number as course_number
    from {{ ref('int__mitxonline__courses') }}
)

, edx_courseruns as (
    select
        *
        , row_number() over (
            partition by trim(course_number)
            order by courserun_start_date desc
        ) as course_recency_rank
    from {{ ref('int__edxorg__mitx_courseruns') }}
)

, edx_courses as (
    select
        courserun_title as course_title
        , micromasters_program_id
        , micromasters_course_id
        , course_number
    from edx_courseruns
    where course_recency_rank = 1
)

, mitx_courses as (
    select
        mitxonline_courses.mitxonline_course_id
        , edx_courses.micromasters_course_id
        , coalesce(mitxonline_courses.course_number, edx_courses.course_number) as course_number
        , coalesce(mitxonline_courses.course_title, edx_courses.course_title) as course_title
        , coalesce(mitxonline_courses.course_number is not null, false) as is_on_mitxonline
        , coalesce(edx_courses.course_number is not null, false) as is_on_edxorg
    from mitxonline_courses
    full join edx_courses
        on mitxonline_courses.course_number = edx_courses.course_number
)

select * from mitx_courses
