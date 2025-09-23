with
    video_structure as (
        select
            *,
            json_query(coursestructure_block_metadata, 'lax $.edx_video_id' omit quotes) as video_edx_uuid,
            element_at(split(coursestructure_block_id, '@'), -1) as video_id
        from {{ ref("int__combined__course_structure") }}
        where coursestructure_block_category = 'video'
    ),
    combined_videos as (

        select '{{ var("mitxonline") }}' as platform, courserun_readable_id, video_edx_uuid, video_duration
        from {{ ref("int__mitxonline__courserun_videos") }}

        union all

        select '{{ var("mitxpro") }}' as platform, courserun_readable_id, video_edx_uuid, video_duration
        from {{ ref("int__mitxpro__courserun_videos") }}

        union all

        select '{{ var("residential") }}' as platform, courserun_readable_id, video_edx_uuid, video_duration
        from {{ ref("int__mitxresidential__courserun_videos") }}

        union all

        select
            '{{ var("edxorg") }}' as platform,
            courserun_old_readable_id as courserun_readable_id,
            video_block_id as video_edx_uuid,
            video_duration
        from {{ ref("stg__edxorg__s3__course_video") }}

    )

select
    video_structure.platform,
    video_structure.courserun_readable_id,
    video_structure.video_id,
    combined_videos.video_edx_uuid as video_edx_id,
    video_structure.coursestructure_block_id as video_block_id,
    video_structure.coursestructure_parent_block_id as video_parent_block_id,
    video_structure.coursestructure_block_metadata as video_metadata,
    video_structure.coursestructure_block_title as video_title,
    video_structure.coursestructure_block_index as video_index,
    video_structure.coursestructure_chapter_title as chapter_title,
    video_structure.coursestructure_chapter_id as chapter_id,
    combined_videos.video_duration
from video_structure
left join
    combined_videos
    on video_structure.courserun_readable_id = combined_videos.courserun_readable_id
    and video_structure.platform = combined_videos.platform
    and (
        video_structure.video_edx_uuid = combined_videos.video_edx_uuid
        or video_structure.video_id = combined_videos.video_edx_uuid
    )
