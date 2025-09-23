with
    video as (select * from {{ ref("int__mitxonline__user_courseactivity_video") }}),
    video_structure as (
        select
            *,
            json_query(coursestructure_block_metadata, 'lax $.edx_video_id' omit quotes) as video_edx_uuid,
            element_at(split(coursestructure_block_id, '@'), -1) as video_id
        from {{ ref("int__mitxonline__course_structure") }}
        where coursestructure_block_category = 'video' and coursestructure_is_latest = true
    ),
    mitxonline_videos as (select * from {{ ref("int__mitxonline__courserun_videos") }}),
    course_runs as (select * from {{ ref("int__mitxonline__course_runs") }}),
    users as (select * from {{ ref("int__mitxonline__users") }})

select
    video.user_username,
    video.courserun_readable_id,
    video.useractivity_video_id as video_id,
    mitxonline_videos.video_edx_uuid,
    video_structure.coursestructure_block_title as video_title,
    video_structure.coursestructure_chapter_title as section_title,
    video_structure.coursestructure_block_index,
    video.useractivity_page_url as page_url,
    video.useractivity_event_type as video_event_type,
    video.useractivity_timestamp as video_event_timestamp,
    video.useractivity_video_currenttime as video_currenttime,
    video.useractivity_video_old_time as video_old_time,
    video.useractivity_video_new_time as video_new_time,
    users.user_full_name,
    users.user_email,
    course_runs.courserun_title,
    course_runs.course_number,
    course_runs.courserun_start_on,
    course_runs.courserun_end_on,
    coalesce(mitxonline_videos.video_duration, video.useractivity_video_duration) as video_duration
from video
inner join course_runs on video.courserun_readable_id = course_runs.courserun_readable_id
left join
    video_structure
    on video.courserun_readable_id = video_structure.courserun_readable_id
    and video.useractivity_video_id = video_structure.video_id
left join
    mitxonline_videos
    on video_structure.courserun_readable_id = mitxonline_videos.courserun_readable_id
    and video_structure.video_edx_uuid = mitxonline_videos.video_edx_uuid
left join users on video.user_username = users.user_username and video.openedx_user_id = users.openedx_user_id
where video.useractivity_event_type in ('play_video', 'seek_video', 'complete_video', 'pause_video', 'stop_video')
