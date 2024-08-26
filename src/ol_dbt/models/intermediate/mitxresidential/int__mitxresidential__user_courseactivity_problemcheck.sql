{{ config(materialized='view') }}

with course_activities as (
    select * from {{ ref('stg__mitxresidential__openedx__tracking_logs__user_activity') }}
    where courserun_readable_id is not null
)

select distinct
    user_username
    , courserun_readable_id
    , user_id
    , useractivity_event_type
    , useractivity_timestamp
    , json_query(useractivity_context_object, 'lax $.module.display_name' omit quotes) as useractivity_problem_name
    , json_query(useractivity_event_object, 'lax $.problem_id' omit quotes) as useractivity_problem_id
    , json_query(useractivity_event_object, 'lax $.answers' omit quotes) as useractivity_problem_student_answers
    , json_query(useractivity_event_object, 'lax $.attempts' omit quotes) as useractivity_problem_attempts
    , json_query(useractivity_event_object, 'lax $.success' omit quotes) as useractivity_problem_success
    , json_query(useractivity_event_object, 'lax $.grade' omit quotes) as useractivity_problem_current_grade
    , json_query(useractivity_event_object, 'lax $.max_grade' omit quotes) as useractivity_problem_max_grade
from course_activities
where useractivity_event_type = 'problem_check'
--- This event emitted by the browser contain all of the GET parameters,
--  only events emitted by the server are useful
and useractivity_event_source = 'server'
