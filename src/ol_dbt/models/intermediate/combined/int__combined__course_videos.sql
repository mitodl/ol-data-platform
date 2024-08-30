with video_structure as (
    select
        *
        , json_query(coursestructure_block_metadata, 'lax $.edx_video_id' omit quotes) as video_edx_uuid
        , element_at(split(coursestructure_block_id, '@'), -1) as video_id
    from {{ ref('int__combined__course_structure') }}
    where coursestructure_block_category = 'video'
)

, mitxonline_videos as (
    select * from {{ ref('int__mitxonline__courserun_videos') }}
)

, xpro_videos as (
    select * from {{ ref('int__mitxpro__courserun_videos') }}
)

, combined as (
    select
        '{{ var("mitxonline") }}' as platform
        , video_structure.courserun_readable_id
        , video_structure.video_id
        , mitxonline_videos.video_edx_uuid as video_edx_id
        , video_structure.coursestructure_block_id as video_block_id
        , video_structure.coursestructure_parent_block_id as video_parent_block_id
        , video_structure.coursestructure_block_metadata as video_metadata
        , video_structure.coursestructure_block_title as video_title
        , video_structure.coursestructure_block_index as video_index
        , video_structure.coursestructure_chapter_title as chapter_title
        , video_structure.coursestructure_chapter_id as chapter_id
        , mitxonline_videos.video_duration
    from video_structure
    left join mitxonline_videos
        on
            video_structure.courserun_readable_id = mitxonline_videos.courserun_readable_id
            and video_structure.video_edx_uuid = mitxonline_videos.video_edx_uuid
    where video_structure.platform = '{{ var("mitxonline") }}'

    union all

    select
        '{{ var("edxorg") }}' as platform
        , courserun_readable_id
        , video_id
        , video_edx_uuid as video_edx_id
        , coursestructure_block_id as video_block_id
        , coursestructure_parent_block_id as video_parent_block_id
        , coursestructure_block_metadata as video_metadata
        , coursestructure_block_title as video_title
        , coursestructure_block_index as video_index
        , coursestructure_chapter_title as chapter_title
        , coursestructure_chapter_id as chapter_id
        --- null until we parse it from course xml for edxorg
        , null as video_duration
    from video_structure
    where video_structure.platform = '{{ var("edxorg") }}'

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , video_structure.courserun_readable_id
        , video_structure.video_id
        , xpro_videos.video_edx_uuid as video_edx_id
        , video_structure.coursestructure_block_id as video_block_id
        , video_structure.coursestructure_parent_block_id as video_parent_block_id
        , video_structure.coursestructure_block_metadata as video_metadata
        , video_structure.coursestructure_block_title as video_title
        , video_structure.coursestructure_block_index as video_index
        , video_structure.coursestructure_chapter_title as chapter_title
        , video_structure.coursestructure_chapter_id as chapter_id
        , xpro_videos.video_duration
    from video_structure
    left join xpro_videos
        on
            video_structure.courserun_readable_id = xpro_videos.courserun_readable_id
            and video_structure.video_edx_uuid = xpro_videos.video_edx_uuid
    where video_structure.platform = '{{ var("mitxpro") }}'
)

select * from combined
