with video_content as (
    select
        content_block_pk
        , block_id
        , block_title
        , courserun_readable_id
        , element_at(split(block_id, '@'), -1) as video_id
        , nullif(json_query(block_metadata, 'lax $.display_name' omit quotes), 'null') as video_name
        , nullif(json_query(block_metadata, 'lax $.start' omit quotes), 'null') as start_date
        , nullif(json_query(block_metadata, 'lax $.due' omit quotes), 'null') as due_date
        , nullif(json_query(block_metadata, 'lax $.edx_video_id' omit quotes), 'null') as edx_video_id
        , nullif(json_query(block_metadata, 'lax $.html5_sources' omit quotes), 'null') as html5_sources
        , nullif(json_query(block_metadata, 'lax $.transcripts' omit quotes), 'null') as transcripts
        , nullif(json_query(block_metadata, 'lax $.edx_video_id' omit quotes), 'null') as video_edx_uuid
        , row_number() over (
            partition by block_id
            order by is_latest desc, retrieved_at desc
        ) as row_num
    from {{ ref('dim_course_content') }}
    where block_category = 'video'
)

, video as (

    select
        video_edx_uuid
        , video_duration
        , video_created_on
    from {{ ref('stg__mitxonline__openedx__mysql__edxval_video') }}

    union all

    select
        video_edx_uuid
        , video_duration
        , video_created_on
    from {{ ref('stg__mitxpro__openedx__mysql__edxval_video') }}

    union all

    select
        video_edx_uuid
        , video_duration
        , video_created_on
    from {{ ref('stg__mitxresidential__openedx__edxval_video') }}

    union all

    select
        video_block_id as video_edx_uuid
        , video_duration
        , null as video_created_on
    from {{ ref('stg__edxorg__s3__course_video') }}

)

, video_duration as (
    select
        video_edx_uuid
        , video_duration
        , row_number() over (
            partition by video_edx_uuid
            order by video_created_on desc
        ) as row_num
    from video
)

, combined as (
    select
        video_content.block_id as video_block_pk
        , video_content.video_id
        , video_content.content_block_pk as content_block_fk
        , video_content.courserun_readable_id
        , video_content.video_edx_uuid
        , video_duration.video_duration
        , video_content.video_name
        , video_content.start_date
        , video_content.due_date
        , video_content.edx_video_id
        , video_content.html5_sources
        , video_content.transcripts
    from video_content
    left join video_duration
        on
            video_content.video_edx_uuid = video_duration.video_edx_uuid
            and video_duration.row_num = 1
    where video_content.row_num = 1
)

select
    video_block_pk
    , content_block_fk
    , courserun_readable_id
    , video_edx_uuid
    , video_name
    , start_date
    , due_date
    , edx_video_id
    , html5_sources
    , transcripts
    , video_duration
from combined
