--- This table captures the historical changes made to course structure. It does it by comparing to course content from
--- previous day. If no changes, then no new rows added to the table. If any changes to the course content, it adds all
--  the blocks including the updated/new ones to the table.
--- To get the course structure at any point in time, use course_id + block_id + the last retrieved_at that close to
--  that time

with course_structure_source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__api__course_structure') }}
)

, course_block_source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__api__course_blocks') }}
)

, course_structure as (
    select * from (
        select
            *
            , lag(content_hash) over (partition by course_id order by retrieved_at asc)
                as previous_content_hash
        from course_structure_source
    )
    where previous_content_hash is null or previous_content_hash != content_hash
)

, course_block as (
    select * from (
        select
            *
            , lag(course_content_hash) over (partition by block_id, course_id order by retrieved_at asc)
                as previous_content_hash
        from course_block_source
    )
    where previous_content_hash is null or previous_content_hash != course_content_hash
)

, cleaned as (
    select
        course_structure.course_id as courserun_readable_id
        , course_block.course_title as courserun_title
        , course_block.block_index as coursestructure_block_index
        , course_block.block_id as coursestructure_block_id
        , course_block.block_parent as coursestructure_parent_block_id
        , course_block.block_type as coursestructure_block_category
        , course_block.block_title as coursestructure_block_title
        , course_structure.content_hash as coursestructure_content_hash
        , course_block.block_content_hash as coursestructure_block_content_hash
        , json_query(course_block.block_details, 'lax $.metadata') as coursestructure_block_metadata
        , {{ cast_timestamp_to_iso8601('course_block.course_start') }} as courserun_start_on
        , {{ cast_timestamp_to_iso8601('course_structure.retrieved_at') }} as coursestructure_retrieved_at
    from course_block
    inner join course_structure
        on
            course_block.course_id = course_structure.course_id
            and course_block.course_content_hash = course_structure.content_hash
)

select * from cleaned
