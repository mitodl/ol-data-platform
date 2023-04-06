-- Course information for MITxPro

with courses as (
    select * from dev.main_staging.stg__mitxpro__app__postgres__courses_course
)

select
    course_id
    , program_id
    , course_title
    , course_is_live
    , course_readable_id
from courses
