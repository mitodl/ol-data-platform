with
    coursevideos as (select * from {{ ref("stg__mitxresidential__openedx__edxval_coursevideo") }}),
    videos as (select * from {{ ref("stg__mitxresidential__openedx__edxval_video") }})

select
    coursevideos.courserun_readable_id,
    coursevideos.coursevideo_is_hidden,
    videos.video_edx_uuid,
    videos.video_client_id,
    videos.video_status,
    videos.video_duration
from coursevideos
inner join videos on coursevideos.video_id = videos.video_id
