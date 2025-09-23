{{ config(materialized="view") }}

with
    course_activities as (
        select *
        from {{ ref("stg__mitxonline__openedx__tracking_logs__user_activity") }}
        where courserun_readable_id is not null
    )

select
    user_username,
    courserun_readable_id,
    openedx_user_id,
    useractivity_path,
    useractivity_timestamp,
    json_query(useractivity_event_object, 'lax $.problem_id' omit quotes) as useractivity_problem_id
from course_activities
where useractivity_event_type = 'showanswer'
