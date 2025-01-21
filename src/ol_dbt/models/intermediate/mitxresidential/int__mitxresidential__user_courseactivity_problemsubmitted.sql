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
    , json_query(useractivity_event_object, 'lax $.event_transaction_id' omit quotes) as useractivity_event_id
    , json_query(useractivity_context_object, 'lax $.module.display_name' omit quotes) as useractivity_problem_name
    , json_query(useractivity_event_object, 'lax $.problem_id' omit quotes) as useractivity_problem_id
    , json_query(useractivity_event_object, 'lax $.weight' omit quotes) as useractivity_problem_weight
    , json_query(useractivity_event_object, 'lax $.weighted_earned' omit quotes) as useractivity_problem_earned_score
    , json_query(useractivity_event_object, 'lax $.weighted_possible' omit quotes) as useractivity_problem_max_score
from course_activities
where useractivity_event_type = 'edx.grades.problem.submitted'
