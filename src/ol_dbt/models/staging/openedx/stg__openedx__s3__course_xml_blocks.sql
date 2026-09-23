with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__openedx__s3__course_xml_blocks") }})

    {{
        deduplicate_raw_table(
            raw_table="raw__openedx__s3__course_xml_blocks",
            partition_columns="source_system, course_id, block_id, block_type",
        )
    }},
    -- Raw appends one file per course version, so a block removed from a course
    -- is still there from the older file. Keeping only the rows from each
    -- course's newest file drops it.
    latest_course_file as (
        select source_system, course_id, max(_file_modified_at) as _file_modified_at
        from source
        group by source_system, course_id
    ),
    current_blocks as (
        select most_recent_source.*
        from most_recent_source
        inner join
            latest_course_file
            on most_recent_source.source_system = latest_course_file.source_system
            and most_recent_source.course_id = latest_course_file.course_id
            and most_recent_source._file_modified_at = latest_course_file._file_modified_at
    ),
    cleaned as (
        select
            course_id as courserun_readable_id,
            source_system as coursestructure_xml_source_system,
            block_id as coursestructure_xml_block_id,
            block_type as coursestructure_xml_block_type,
            block_display_name as coursestructure_xml_block_display_name,
            xml_attributes as coursestructure_xml_block_attributes,
            xml_path as coursestructure_xml_block_path,
            raw_xml as coursestructure_xml_raw_xml,
            edx_video_id as video_edx_id,
            cast(nullif(trim(duration), '') as decimal(38, 4)) as video_duration,
            max_attempts as problem_max_attempts,
            weight as problem_weight,
            markdown as problem_markdown,
            {{ cast_timestamp_to_iso8601("retrieved_at") }} as coursestructure_xml_retrieved_at
        from current_blocks
    )

select *
from cleaned
