{{ config(full_refresh = true) }}

with mitxonline_video_engagements as (
    select
        user_email
        , section_title
        , courserun_readable_id
        , video_title
        , video_event_type
        , coursestructure_block_index
        , {{ is_courserun_current('courserun_start_on', 'courserun_end_on') }} as courserun_is_current
    from {{ ref('marts__mitxonline_video_engagements') }}
)

, mitx__courses as (
    select * from {{ ref('int__mitx__courses') }}
)

, subq_video_engagements as (
    select
        section_title
        , courserun_readable_id
        , count(distinct video_title) as total_video_count
    from mitxonline_video_engagements
    group by
        section_title
        , courserun_readable_id
)

select
    mitxonline_video_engagements.user_email
    , mitxonline_video_engagements.section_title
    , mitxonline_video_engagements.courserun_readable_id
    , subq_video_engagements.total_video_count
    , 'MITx Online' as platform
    , mitxonline_video_engagements.courserun_is_current
    , count(distinct mitxonline_video_engagements.video_title) as user_watched_video_count
    , max(mitxonline_video_engagements.coursestructure_block_index) as max_coursestructure_block_index
from mitxonline_video_engagements
join subq_video_engagements
    on
        mitxonline_video_engagements.section_title = subq_video_engagements.section_title
        and mitxonline_video_engagements.courserun_readable_id = subq_video_engagements.courserun_readable_id
where mitxonline_video_engagements.video_event_type = 'play_video'
group by
    mitxonline_video_engagements.user_email
    , mitxonline_video_engagements.section_title
    , mitxonline_video_engagements.courserun_readable_id
    , subq_video_engagements.total_video_count
    , 'MITx Online'
    , mitxonline_video_engagements.courserun_is_current
