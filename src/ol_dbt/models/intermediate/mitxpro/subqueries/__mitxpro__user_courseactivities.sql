{{ config(materialized='view') }}

with course_activities as (
    select * from {{ ref('stg__mitxpro__openedx__tracking_logs__user_activity') }}
    where courserun_readable_id is not null
)

select
    user_username
    , courserun_readable_id
    , openedx_user_id
    , org_id
    , useractivity_event_source
    , useractivity_event_name
    , useractivity_event_type
    , useractivity_context_object
    , useractivity_event_object
    , useractivity_path
    , useractivity_page_url
    , useractivity_session_id
    , useractivity_ip
    , useractivity_timestamp
from course_activities
