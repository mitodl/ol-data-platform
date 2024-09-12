-- Course information for MITx Online

with courses as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__courses_course') }}
)

, course_pages as (
    select * from {{ ref('stg__mitxonline__app__postgres__cms_coursepage') }}
)

select
    courses.course_id
    , courses.course_title
    , courses.course_is_live
    , courses.course_readable_id
    , courses.course_number
    , course_pages.course_description
    , course_pages.course_length
    , course_pages.course_effort
    , course_pages.course_prerequisites
    , course_pages.course_about
    , course_pages.course_what_you_learn
from courses
left join course_pages on courses.course_id = course_pages.course_id
