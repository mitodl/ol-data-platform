with course_block_source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__api__course_blocks') }}
)

-- A retrieval is kept when the course's content hash differs from its previous retrieval's. The hash covers the
-- whole course, so it is compared per course and not per block: a block that is removed and later restored would
-- otherwise compare its restored copy with its own last copy, match, and be dropped from the restoring retrieval.
-- The hash breaks ties between two retrievals at the same timestamp.
, course_retrieval as (
    select * from (
        select
            course_id
            , retrieved_at
            , course_content_hash
            , lag(course_content_hash) over (
                partition by course_id
                order by retrieved_at asc, course_content_hash asc
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

, course_block as (
    select course_block_source.*
    from course_block_source
    inner join course_retrieval
        on
            course_block_source.course_id = course_retrieval.course_id
            and course_block_source.retrieved_at = course_retrieval.retrieved_at
            and course_block_source.course_content_hash = course_retrieval.course_content_hash
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
