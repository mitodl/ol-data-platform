-- Course information for MITx Online

with courses as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__courses_course') }}
)

select
    course_id
    , course_title
    , course_is_live
    , course_readable_id
    , course_number
from courses
