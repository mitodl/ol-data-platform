with ocw_courses as (
    select
        course_pk
        , course_readable_id
        , course_title
        , course_number
        , course_is_live
    from {{ ref('dim_course') }}
    where primary_platform = 'ocw'
        and is_current = true
)

, ocw_course_details as (
    select
        course_readable_id
        , course_name
        , course_term
        , course_year
        , course_level
        , course_extra_course_numbers
        , course_is_unpublished
        , course_has_never_published
        , course_live_url
        , course_first_published_on
        , course_publish_date_updated_on
        , course_topics
        , course_learning_resource_types
        , course_department_numbers
    from {{ ref('dim_ocw_course') }}
)

, instructors as (
    select
        bridge_course_instructor.course_fk
        , {{ array_join('array_agg(dim_instructor.instructor_name)', ", ") }} as course_instructors
    from {{ ref('bridge_course_instructor') }}
    inner join {{ ref('dim_instructor') }}
        on bridge_course_instructor.instructor_fk = dim_instructor.instructor_pk
    group by bridge_course_instructor.course_fk
)

select
    ocw_courses.course_readable_id as course_short_id
    , ocw_course_details.course_name
    , ocw_courses.course_title
    , ocw_course_details.course_term
    , ocw_course_details.course_year
    , ocw_course_details.course_level
    , ocw_courses.course_number as course_primary_course_number
    , ocw_course_details.course_extra_course_numbers
    , ocw_courses.course_is_live
    , ocw_course_details.course_is_unpublished
    , ocw_course_details.course_has_never_published
    , ocw_course_details.course_live_url
    , ocw_course_details.course_first_published_on
    , ocw_course_details.course_publish_date_updated_on
    , ocw_course_details.course_topics
    , ocw_course_details.course_learning_resource_types
    , ocw_course_details.course_department_numbers
    , instructors.course_instructors
from ocw_courses
inner join ocw_course_details
    on ocw_courses.course_readable_id = ocw_course_details.course_readable_id
left join instructors
    on ocw_courses.course_pk = instructors.course_fk
