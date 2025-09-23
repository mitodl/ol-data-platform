with
    mitxpro_coursevideos as (select * from {{ ref("stg__mitxpro__openedx__mysql__edxval_coursevideo") }}),
    mitxpro_videos as (select * from {{ ref("stg__mitxpro__openedx__mysql__edxval_video") }}),
    ovs_videos as (
        select *, row_number() over (partition by video_title order by video_created_on desc) as video_rank
        from {{ ref("int__ovs__videos") }}
        where platform = '{{ var("mitxpro") }}'
    )

select
    mitxpro_coursevideos.courserun_readable_id,
    mitxpro_coursevideos.coursevideo_is_hidden,
    mitxpro_videos.video_edx_uuid,
    mitxpro_videos.video_client_id,
    mitxpro_videos.video_status,
    coalesce(cast(ovs_videos.video_duration as decimal(38, 4)), mitxpro_videos.video_duration) as video_duration
from mitxpro_coursevideos
inner join mitxpro_videos on mitxpro_coursevideos.video_id = mitxpro_videos.video_id
-- - the relationship between OVS videos and xPro videos are linked via video title and client video ID
-- e.g. TTSBSV3_Web3_BVB_FINAL.mp4. If there are multiple videos with same title on OVS, use the latest one.
left join ovs_videos on mitxpro_videos.video_client_id = ovs_videos.video_title and ovs_videos.video_rank = 1
