with
    video_encodejobs as (
        select
            *,
            -- - there may be multiple encodejobs for the same video, but we only need the last one for video duration
            row_number() over (partition by video_id order by encodejob_created_on desc) as encodejob_rank
        from {{ ref("stg__ovs__studio__postgres__ui_encodejob") }}
    ),
    videos as (select * from {{ ref("stg__ovs__studio__postgres__ui_video") }}),
    collections as (select * from {{ ref("stg__ovs__studio__postgres__ui_collection") }}),
    collection_edxendpoints as (select * from {{ ref("stg__ovs__studio__postgres__ui_collectionedxendpoint") }}),
    edxendpoints as (select * from {{ ref("stg__ovs__studio__postgres__ui_edxendpoint") }})

select
    collections.collection_id,
    collections.collection_uuid,
    collections.collection_title,
    collections.courserun_readable_id,
    edxendpoints.edxendpoint_base_url,
    videos.video_id,
    videos.video_uuid,
    videos.video_title,
    videos.video_status,
    videos.video_created_on,
    video_encodejobs.video_duration,
    case
        when edxendpoints.edxendpoint_base_url = '{{ var("mitxonline_openedx_url") }}'
        then '{{ var("mitxonline") }}'
        when edxendpoints.edxendpoint_base_url = '{{ var("mitxpro_openedx_url") }}'
        then '{{ var("mitxpro") }}'
    end as platform
from videos
inner join collections on videos.collection_id = collections.collection_id
inner join collection_edxendpoints on collections.collection_id = collection_edxendpoints.collection_id
inner join edxendpoints on collection_edxendpoints.edxendpoint_id = edxendpoints.edxendpoint_id
left join video_encodejobs on videos.video_id = video_encodejobs.video_id and video_encodejobs.encodejob_rank = 1
where videos.video_status = 'Complete'
