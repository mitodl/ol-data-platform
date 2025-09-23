with
    course_block_source as (
        select * from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__api__course_blocks") }}
    ),
    course_block as (
        select *
        from
            (
                select
                    course_block_source.*,
                    lag(course_block_source.course_content_hash) over (
                        partition by course_block_source.block_id, course_block_source.course_id
                        order by course_block_source.retrieved_at asc
                    ) as previous_content_hash
                from course_block_source
            )
        where previous_content_hash is null or previous_content_hash != course_content_hash
    ),
    cleaned as (
        select
            course_id as courserun_readable_id,
            course_title as courserun_title,
            block_index as coursestructure_block_index,
            block_id as coursestructure_block_id,
            block_parent as coursestructure_parent_block_id,
            block_type as coursestructure_block_category,
            block_title as coursestructure_block_title,
            course_content_hash as coursestructure_content_hash,
            block_content_hash as coursestructure_block_content_hash,
            json_query(block_details, 'lax $.metadata') as coursestructure_block_metadata,
            {{ cast_timestamp_to_iso8601("course_start") }} as courserun_start_on,
            {{ cast_timestamp_to_iso8601("retrieved_at") }} as coursestructure_retrieved_at
        from course_block
    )

select *
from cleaned
