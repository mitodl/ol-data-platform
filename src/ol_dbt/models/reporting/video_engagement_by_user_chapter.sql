{{
    config(
        materialized='view'
    )
}}

{#
    Reporting view for video engagements with pre-aggregated video count metrics.
    Replaces Superset virtual dataset: marts__combined_video_engagements_w_video_counts

    Provides per-user, per-chapter aggregations including:
    - user_watched_video_count: count of distinct videos the user watched
    - total_video_count: total distinct videos available in the chapter
    - max_video_index: highest video index watched by the user
#}

with video_engagements as (
    select * from {{ ref('marts__combined_video_engagements') }}
)

, chapter_video_totals as (
    select
        chapter_title
        , courserun_readable_id
        , count(distinct video_title) as total_video_count
    from video_engagements
    group by 1, 2
)

select
    video_engagements.user_email
    , video_engagements.chapter_title
    , video_engagements.courserun_readable_id
    , chapter_video_totals.total_video_count
    , video_engagements.platform
    , video_engagements.courserun_is_current
    , video_engagements.course_readable_id
    , count(distinct video_engagements.video_title) as user_watched_video_count
    , max(video_engagements.video_index) as max_video_index
from video_engagements
inner join chapter_video_totals
    on video_engagements.chapter_title = chapter_video_totals.chapter_title
    and video_engagements.courserun_readable_id = chapter_video_totals.courserun_readable_id
where video_engagements.video_event_type = 'play_video'
group by 1, 2, 3, 4, 5, 6, 7
