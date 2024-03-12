with grades_persistentsubsectiongrade as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__grades_persistentsubsectiongrade') }}
)

select
    course_id
    , user_id
    , usage_key
    , earned_all
    , possible_all
    , earned_graded
    , possible_graded
    , first_attempted
    , created
    , modified
from grades_persistentsubsectiongrade
order by user_id, first_attempted
