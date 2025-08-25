with combined_video_engagements as (
    select
        '{{ var("mitxonline") }}' as platform
        , user_username
        , courserun_readable_id
        , useractivity_video_id
        , useractivity_page_url
        , useractivity_event_type
        , useractivity_timestamp
        , useractivity_video_duration
        , useractivity_video_currenttime
        , useractivity_video_old_time
        , useractivity_video_new_time
    from {{ ref('int__mitxonline__user_courseactivity_video') }}

    union all

    select
        '{{ var("edxorg") }}' as platform
        , user_username
        , courserun_readable_id
        , useractivity_video_id
        , useractivity_page_url
        , useractivity_event_type
        , useractivity_timestamp
        , useractivity_video_duration
        , useractivity_video_currenttime
        , useractivity_video_old_time
        , useractivity_video_new_time
    from {{ ref('int__edxorg__mitx_user_courseactivity_video') }}

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , user_username
        , courserun_readable_id
        , useractivity_video_id
        , useractivity_page_url
        , useractivity_event_type
        , useractivity_timestamp
        , useractivity_video_duration
        , useractivity_video_currenttime
        , useractivity_video_old_time
        , useractivity_video_new_time
    from {{ ref('int__mitxpro__user_courseactivity_video') }}

    union all

    select
        '{{ var("residential") }}' as platform
        , user_username
        , courserun_readable_id
        , useractivity_video_id
        , useractivity_page_url
        , useractivity_event_type
        , useractivity_timestamp
        , useractivity_video_duration
        , useractivity_video_currenttime
        , useractivity_video_old_time
        , useractivity_video_new_time
    from {{ ref('int__mitxresidential__user_courseactivity_video') }}
)

, combined_video_structure as (
    select * from {{ ref('int__combined__course_videos') }}
)

, combined_runs as (
    select * from {{ ref('int__combined__course_runs') }}
)

, combined_users as (
    select * from {{ ref('int__combined__users') }}

)

select
    combined_video_engagements.platform
    , combined_video_engagements.user_username
    , combined_video_engagements.courserun_readable_id
    , combined_video_engagements.useractivity_video_id as video_id
    , combined_video_structure.video_edx_id
    , combined_video_structure.video_title
    , combined_video_structure.video_index
    , combined_video_structure.chapter_title
    , combined_video_structure.chapter_id
    , combined_video_engagements.useractivity_page_url as page_url
    , combined_video_engagements.useractivity_event_type as video_event_type
    , combined_video_engagements.useractivity_timestamp as video_event_timestamp
    , combined_video_engagements.useractivity_video_currenttime as video_currenttime
    , combined_video_engagements.useractivity_video_old_time as video_old_time
    , combined_video_engagements.useractivity_video_new_time as video_new_time
    , combined_users.user_hashed_id
    , combined_users.user_full_name
    , combined_users.user_email
    , combined_users.user_address_country as user_country_code
    , combined_users.user_highest_education
    , combined_users.user_gender
    , combined_runs.courserun_title
    , combined_runs.course_readable_id
    , combined_runs.course_number
    , combined_runs.courserun_is_current
    , combined_runs.courserun_start_on
    , combined_runs.courserun_end_on
    , coalesce(
        combined_video_structure.video_duration
        , combined_video_engagements.useractivity_video_duration
    ) as video_duration
from combined_video_engagements
inner join combined_runs
    on
        combined_video_engagements.courserun_readable_id = combined_runs.courserun_readable_id
        and combined_video_engagements.platform = combined_runs.platform
left join combined_video_structure
    on
        combined_video_engagements.courserun_readable_id = combined_video_structure.courserun_readable_id
        and combined_video_engagements.useractivity_video_id = combined_video_structure.video_id
        and combined_video_engagements.platform = combined_video_structure.platform
left join combined_users
    on
        combined_video_engagements.user_username = combined_users.user_username
        and combined_video_engagements.platform = combined_users.platform
where
    combined_video_engagements.useractivity_event_type
    in ('play_video', 'seek_video', 'complete_video', 'pause_video', 'stop_video')
