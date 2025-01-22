with mitxonline_coursevideos as (
    select *
    from {{ ref('stg__mitxonline__openedx__mysql__edxval_coursevideo') }}
)

, mitxonline_videos as (
    select * from {{ ref('stg__mitxonline__openedx__mysql__edxval_video') }}
)

, ovs_videos as (
    select
        *
        , row_number() over (
            partition by video_title
            order by video_created_on desc
        ) as video_rank
    from {{ ref('int__ovs__videos') }}
    where platform = '{{ var("mitxonline") }}'
)


select
    mitxonline_coursevideos.courserun_readable_id
    , mitxonline_coursevideos.coursevideo_is_hidden
    , mitxonline_videos.video_edx_uuid
    , mitxonline_videos.video_client_id
    , mitxonline_videos.video_status
    , coalesce(cast(ovs_videos.video_duration as decimal(38, 4)), mitxonline_videos.video_duration) as video_duration
from mitxonline_coursevideos
inner join mitxonline_videos on mitxonline_coursevideos.video_id = mitxonline_videos.video_id
--- the relationship between OVS videos and MITx Online videos are linked via video title and client video ID
-- e.g. 14.310x_Lect9_Seg8_Final.mp4. If there are multiple videos with same title on OVS, use the latest one.
left join ovs_videos on mitxonline_videos.video_client_id = ovs_videos.video_title and ovs_videos.video_rank = 1
