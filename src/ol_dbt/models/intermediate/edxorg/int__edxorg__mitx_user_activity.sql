{{ config(
    materialized="incremental",
    unique_key=['user_username', 'useractivity_context_object', 'useractivity_event_source',
    'useractivity_event_type', 'useractivity_event_object', 'useractivity_timestamp'],
    incremental_strategy='delete+insert',
    views_enabled=false
) }}

select
    user_username
    , user_id
    , useractivity_path
    , useractivity_context_object
    , useractivity_event_source
    , useractivity_event_type
    , useractivity_event_object
    , useractivity_event_name
    , useractivity_page_url
    , useractivity_timestamp
    , {{ format_course_id('courserun_readable_id') }} as courserun_readable_id
from {{ ref('stg__edxorg__s3__tracking_logs__user_activity') }}
where
    courserun_readable_id is not null
    {% if is_incremental() %}
        -- An empty target (e.g. a first build with no course activity) would make max() NULL
        -- and filter out every later row, so fall back to '' which every ISO string exceeds.
        and useractivity_timestamp > (
            select coalesce(max(this.useractivity_timestamp), '') from {{ this }} as this
        )
    {% endif %}
