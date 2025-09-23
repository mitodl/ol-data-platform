with
    video_engagements as (select * from {{ ref("marts__combined_video_engagements") }}),
    video_engagements_total as (
        select chapter_title, courserun_readable_id, count(distinct video_title) as total_video_count
        from {{ ref("marts__combined_video_engagements") }}
        group by chapter_title, courserun_readable_id
    )

select
    video_engagements.user_email,
    video_engagements.chapter_title,
    video_engagements.courserun_readable_id,
    video_engagements_total.total_video_count,
    video_engagements.platform,
    video_engagements.courserun_is_current,
    video_engagements.course_readable_id,
    count(distinct video_engagements.video_title) as user_watched_video_count,
    max(video_engagements.video_index) as max_video_index
from video_engagements
inner join
    video_engagements_total
    on video_engagements.chapter_title = video_engagements_total.chapter_title
    and video_engagements.courserun_readable_id = video_engagements_total.courserun_readable_id
where video_engagements.video_event_type = 'play_video'
group by
    video_engagements.user_email,
    video_engagements.chapter_title,
    video_engagements.courserun_readable_id,
    video_engagements_total.total_video_count,
    video_engagements.platform,
    video_engagements.courserun_is_current,
    video_engagements.course_readable_id
