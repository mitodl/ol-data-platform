with grades_persistentcoursegrade as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__grades_persistentcoursegrade') }}
)

select
    course_id
    , user_id
    , grading_policy_hash
    , percent_grade
    , letter_grade
    , passed_timestamp
    , created
    , modified
from grades_persistentcoursegrade
order by user_id
