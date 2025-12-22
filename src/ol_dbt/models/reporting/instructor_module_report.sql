with chatbot_events as (
    select * from {{ ref('tfact_chatbot_events') }}
)

, course_content as (
    select * from {{ ref('dim_course_content') }}
)

, user as (
    select * from {{ ref('dim_user') }}
)

, enrollment_detail as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, video_events as (
    select * from {{ ref('tfact_video_events') }}
)

, video as (
    select * from {{ ref('dim_video') }}
)

, org_data as (
    select * from {{ ref('organization_administration_report') }}
)

, any_activity as (
    select
        user_email
        , courserun_readable_id
    from org_data
    where active_count = 1
    group by
        user_email
        , courserun_readable_id
)

, enroll_activity as (
    select
        user_email
        , courserun_readable_id
    from enrollment_detail
    where courserunenrollment_enrollment_status is null
    group by
        user_email
        , courserun_readable_id
)

, video_pre_query as (
    select
        video_events.user_fk
        , video_events.courserun_readable_id
        , video_events.video_block_fk
        , max(video_events.video_duration) as video_duration
        , max(video_events.video_position) as end_time
        , min(case when video_events.event_type = 'play_video' then video_events.video_position end) as start_time
    from video_events
    inner join user
        on video_events.user_fk = user.user_pk
    where
        video_events.event_type in (
            'play_video'
            , 'seek_video'
            , 'pause_video'
            , 'stop_video'
            , 'complete_video'
        )
    group by
        video_events.user_fk
        , video_events.courserun_readable_id
        , video_events.video_block_fk
)

, video_watches as (
    select
        user.email
        , video_events.courserun_readable_id
        , video_events.video_block_fk
        , cast(video_events.event_timestamp as date) as activity_date
        , lag(cast(video_events.event_timestamp as date)) over (partition by user.email
        , video_events.courserun_readable_id
        , video_events.video_block_fk order by cast(video_events.event_timestamp as date)) AS PreviousDATE
    from video_events
    inner join user
        on video_events.user_fk = user.user_pk
    where
        video_events.event_type in (
            'play_video'
        )
    group by
        user.email
        , video_events.courserun_readable_id
        , video_events.video_block_fk
        , cast(video_events.event_timestamp as date)
)

, video_views_table as (
    select
        a.courserun_readable_id
        , a.video_block_fk
        , v.block_category
        , v.block_title
        , cc_section.block_title as section_title
        , cc_subsection.block_title as subsection_title
        , user.email
        , sum(
            cast(case when a.end_time = 'null' then '0' else a.end_time end as decimal(30, 10))
            - cast(case when a.start_time = 'null' then '0' else a.start_time end as decimal(30, 10))
        )
            as estimated_time_played
        , sum(a.video_duration) as video_duration
    from video_pre_query as a
    inner join video as c
        on
            a.courserun_readable_id = c.courserun_readable_id
            and a.video_block_fk = substring(c.video_block_pk, regexp_position(c.video_block_pk, 'block@') + 6)
    inner join user
        on a.user_fk = user.user_pk
    left join course_content as v
        on
            c.content_block_fk = v.content_block_pk
            and a.courserun_readable_id = v.courserun_readable_id
    left join course_content as cc_subsection
        on
            v.parent_block_id = cc_subsection.block_id
            and a.courserun_readable_id = cc_subsection.courserun_readable_id
            and cc_subsection.is_latest = true
    left join course_content as cc_section
        on
            cc_subsection.parent_block_id = cc_section.block_id
            and a.courserun_readable_id = cc_section.courserun_readable_id
            and cc_section.is_latest = true
    group by
        a.courserun_readable_id
        , a.video_block_fk
        , v.block_category
        , v.block_title
        , cc_section.block_title
        , cc_subsection.block_title
        , user.email
)

, combined_data as (
    select
        video_views_table.email as user_email
        , video_watches.activity_date
        , video_views_table.courserun_readable_id
        , 0 as chatbot_used_count
        , video_views_table.block_category
        , video_views_table.block_title
        , video_views_table.section_title
        , video_views_table.subsection_title
        , null as chatbot_type
        , video_views_table.estimated_time_played
        , video_views_table.video_duration
        , case when video_watches.PreviousDATE is not null then true else false end  as rewatch_indicator
        , 1 as video_watched_count
    from video_views_table
    inner join video_watches
        on
            video_views_table.courserun_readable_id = video_watches.courserun_readable_id
            and video_views_table.video_block_fk = video_watches.video_block_fk
            and video_views_table.email = video_watches.email

    union all

    select
        user.email as user_email
        , cast(chatbot_events.event_timestamp as date) as activity_date
        , chatbot_events.courserun_readable_id
        , count(distinct chatbot_events.session_id || chatbot_events.block_id) as chatbot_used_count
        , c.block_category
        , c.block_title
        , section.block_title as section_title
        , subsection.block_title as subsection_title
        , chatbot_events.chatbot_type
        , null as estimated_time_played
        , null as video_duration
        , null as rewatch_indicator
        , null as video_watched_count
    from chatbot_events
    inner join user
        on chatbot_events.user_fk = user.user_pk
    left join course_content as c
        on
            chatbot_events.block_id = c.block_id
            and c.is_latest = true
    left join course_content as section
        on
            c.chapter_block_id = section.block_id
            and section.is_latest = true
    left join course_content as subsection
        on
            c.sequential_block_id = subsection.block_id
            and subsection.is_latest = true
    where chatbot_events.event_type = 'ol_openedx_chat.drawer.submit'
    group by
        user.email
        , cast(chatbot_events.event_timestamp as date)
        , chatbot_events.courserun_readable_id
        , c.block_category
        , c.block_title
        , section.block_title
        , subsection.block_title
        , chatbot_events.chatbot_type
)

select
    combined_data.user_email
    , combined_data.activity_date
    , combined_data.courserun_readable_id
    , combined_data.chatbot_used_count
    , combined_data.block_category
    , combined_data.block_title
    , combined_data.section_title
    , combined_data.subsection_title
    , combined_data.chatbot_type
    , combined_data.estimated_time_played
    , combined_data.video_duration
    , combined_data.rewatch_indicator
    , combined_data.video_watched_count
    , case when enroll_activity.user_email is not null then true else false end as enrolled_ind
    , case when any_activity.user_email is not null then true else false end as ever_active_ind
from combined_data
left join enroll_activity
    on
        combined_data.user_email = enroll_activity.user_email
        and combined_data.courserun_readable_id = enroll_activity.courserun_readable_id
left join any_activity
    on
        combined_data.user_email = any_activity.user_email
        and combined_data.courserun_readable_id = any_activity.courserun_readable_id
