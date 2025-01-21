with grades_persistentsubsectiongrade as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__grades_persistentsubsectiongrade') }}
)

{{ deduplicate_query('grades_persistentsubsectiongrade', 'most_recent_source') }}

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
from most_recent_source
order by user_id, first_attempted
