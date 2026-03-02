{{ config(materialized='view') }}

with course_activities as (
    select * from {{ ref('stg__mitxpro__openedx__tracking_logs__user_activity') }}
    where courserun_readable_id is not null
)

select
    user_username
    , courserun_readable_id
    , openedx_user_id
    , useractivity_event_type
    , useractivity_timestamp
    , {{ json_query_string('useractivity_context_object', "'$.module.display_name'") }} as useractivity_problem_name
    , {{ json_query_string('useractivity_event_object', "'$.problem_id'") }} as useractivity_problem_id
    , {{ json_query_string('useractivity_event_object', "'$.answers'") }} as useractivity_problem_student_answers
    , {{ json_query_string('useractivity_event_object', "'$.attempts'") }} as useractivity_problem_attempts
    , {{ json_query_string('useractivity_event_object', "'$.success'") }} as useractivity_problem_success
    , {{ json_query_string('useractivity_event_object', "'$.grade'") }} as useractivity_problem_current_grade
    , {{ json_query_string('useractivity_event_object', "'$.max_grade'") }} as useractivity_problem_max_grade
from course_activities
where useractivity_event_type = 'problem_check'
--- This event emitted by the browser contain all of the GET parameters,
--  only events emitted by the server are useful
and useractivity_event_source = 'server'
