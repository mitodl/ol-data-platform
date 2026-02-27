with combined_video_engagements as (
    select * from {{ ref('marts__combined_video_engagements') }}
)

, video_counts_by_chapter as (
    select
        chapter_title
        , courserun_readable_id
        , count(distinct video_title) as total_video_count
    from combined_video_engagements
    group by
        chapter_title
        , courserun_readable_id
)

select
    combined_video_engagements.user_email
    , combined_video_engagements.chapter_title
    , combined_video_engagements.courserun_readable_id
    , video_counts_by_chapter.total_video_count
    , combined_video_engagements.platform
    , combined_video_engagements.courserun_is_current
    , combined_video_engagements.course_readable_id
    , count(distinct combined_video_engagements.video_title) as user_watched_video_count
    , max(combined_video_engagements.video_index) as max_video_index
from combined_video_engagements
inner join video_counts_by_chapter
    on combined_video_engagements.chapter_title = video_counts_by_chapter.chapter_title
    and combined_video_engagements.courserun_readable_id = video_counts_by_chapter.courserun_readable_id
where combined_video_engagements.video_event_type = 'play_video'
group by
    combined_video_engagements.user_email
    , combined_video_engagements.chapter_title
    , combined_video_engagements.courserun_readable_id
    , video_counts_by_chapter.total_video_count
    , combined_video_engagements.platform
    , combined_video_engagements.courserun_is_current
    , combined_video_engagements.course_readable_id
