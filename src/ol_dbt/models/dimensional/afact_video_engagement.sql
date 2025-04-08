with user_course_videos as (
    select
        user_id,
        , courserun_readable_id
        , count(distinct video_id) as num_of_video_played
        , count(video_completion) as num_of_video_completed
        , AVG(video_engagement) as video_engagement
    from afact_video_watched
    group by user_id, courserun_readable_id
)

, course_videos as (
    select
        courserun_readable_id
        , count(distinct video_id) as num_of_video
    from tfact_video_events
    group by courserun_readable_id
)

, course_sections as (
    select
        courserun_readable_id
        , parent_block_id as section_block_id
        , parent.block_title as section_name
        , block_id as subsection_block_id
        , block_title as subsection_name
    from dim_course_content
    left join dim_course_content as parent
        on dim_course_content.parent_block_id = parent.block_id
    where block_category = 'video'
)

, combined as (
    select
        user_course_videos.user_id
        , user_course_videos.courserun_readable_id
        , user_course_videos.video_id as section_block_id
        , course_sections.section_name
        , course_sections.subsection_block_id
        , course_sections.subsection_name
        , course_videos.num_of_video
        , user_course_videos.num_of_video_played
        , user_course_videos.num_of_video_completed
        , user_course_videos.video_engagement
    from user_course_videos
    left join course_videos
        on user_course_videos.courserun_readable_id = course_videos.courserun_readable_id
    left join course_sections
        on user_course_videos.video_id = course_sections.subsection_block_id
)


select distinct
    user_id
    , courserun_readable_id
    , section_block_id
    , section_name
    , subsection_block_id
    , subsection_name
    , num_of_video
    , num_of_video_played
    , num_of_video_completed
    , video_engagement
from combined
