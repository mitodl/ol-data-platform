{{ config(materialized="view") }}

select
    user_username,
    user_id,
    useractivity_path,
    useractivity_context_object,
    useractivity_event_source,
    useractivity_event_type,
    useractivity_event_object,
    useractivity_event_name,
    useractivity_page_url,
    useractivity_timestamp,
    {{ format_course_id("courserun_readable_id") }} as courserun_readable_id
from {{ ref("stg__edxorg__s3__tracking_logs__user_activity") }}
where courserun_readable_id is not null
