with course_block_source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__api__course_structure_blocks') }}
)

-- Keep only retrievals whose course hash changed. The hash is compared per course, not per block, so a block
-- removed and later restored is kept in the retrieval that restores it.
, course_retrieval as (
    select * from (
        select
            course_id
            , retrieved_at
            , course_content_hash
            , lag(course_content_hash) over (
                partition by course_id
                order by retrieved_at asc nulls last, course_content_hash asc nulls last
            ) as previous_content_hash
        from (
            select distinct
                course_id
                , retrieved_at
                , course_content_hash
            from course_block_source
        ) as distinct_retrieval
    ) as ordered_retrieval
    where previous_content_hash is null or previous_content_hash != course_content_hash
)

-- The raw table can hold a block more than once in one retrieval, so only one copy is kept.
, course_block as (
    select * from (
        select
            course_block_source.*
            , row_number() over (
                partition by
                    course_block_source.course_id
                    , course_block_source.block_id
                    , course_block_source.retrieved_at
                    , course_block_source.course_content_hash
                order by course_block_source.block_content_hash asc nulls last, course_block_source.block_index asc nulls last
            ) as block_copy_number
        from course_block_source
        where (course_block_source.course_id, course_block_source.retrieved_at, course_block_source.course_content_hash) in (
            select course_retrieval.course_id, course_retrieval.retrieved_at, course_retrieval.course_content_hash
            from course_retrieval
        )
    ) as retrieved_block
    where block_copy_number = 1
)

, cleaned as (
    select
        course_id as courserun_readable_id
        , course_title as courserun_title
        , block_index as coursestructure_block_index
        , block_id as coursestructure_block_id
        , block_parent as coursestructure_parent_block_id
        , block_type as coursestructure_block_category
        , block_title as coursestructure_block_title
        , course_content_hash as coursestructure_content_hash
        , block_content_hash as coursestructure_block_content_hash
        , {{ json_extract_value('block_details', "'$.metadata'") }} as coursestructure_block_metadata
        , {{ cast_timestamp_to_iso8601('course_start') }} as courserun_start_on
        , {{ cast_timestamp_to_iso8601('retrieved_at') }} as coursestructure_retrieved_at
    from course_block
)

select * from cleaned
