{{ config(materialized='view') }}

with course_activities as (
    select * from {{ ref('stg__mitxresidential__openedx__tracking_logs__user_activity') }}
    where courserun_readable_id is not null
)

select
    user_username
    , courserun_readable_id
    , user_id
    , useractivity_event_source
    , useractivity_event_type
    , useractivity_path
    , useractivity_timestamp
    , {{ json_query_string('useractivity_event_object', "'$.event_transaction_id'") }} as useractivity_event_id
    , {{ json_query_string('useractivity_context_object', "'$.module.display_name'") }} as useractivity_problem_name
    , {{ json_query_string('useractivity_event_object', "'$.problem_id'") }} as useractivity_problem_id
    , {{ json_query_string('useractivity_event_object', "'$.weight'") }} as useractivity_problem_weight
    , {{ json_query_string('useractivity_event_object', "'$.weighted_earned'") }} as useractivity_problem_earned_score
    , {{ json_query_string('useractivity_event_object', "'$.weighted_possible'") }} as useractivity_problem_max_score
from course_activities
where useractivity_event_type = 'edx.grades.problem.submitted'
