with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__edxorg__s3__course_structure__course_video') }}
)

{{ deduplicate_query(cte_name1='source', cte_name2='most_recent_source'
, partition_columns = 'course_id, video_block_id') }}

, cleaned as (

    select
        course_id as courserun_readable_id
        , video_block_id
        , edx_video_id as video_edx_id
        , duration as video_duration
    from most_recent_source
)

select * from cleaned
