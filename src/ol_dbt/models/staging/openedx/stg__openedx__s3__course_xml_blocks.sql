with
    raw_source as (select * from {{ source("ol_warehouse_raw_data", "raw__openedx__s3__course_xml_blocks") }}),
    -- Raw appends one file per course version, so a block removed from a course
    -- is still there from the older file. Only the rows of each course's newest
    -- file are staged. S3 mtimes are whole seconds, so two versions can tie on
    -- _file_modified_at; retrieved_at (when the archive was parsed) and then the
    -- path break the tie, so exactly one file is chosen per course.
    course_files as (
        select
            source_system,
            course_id,
            _source_file,
            max(_file_modified_at) as _file_modified_at,
            max(retrieved_at) as retrieved_at
        from raw_source
        group by source_system, course_id, _source_file
    ),
    ranked_course_files as (
        select
            source_system,
            course_id,
            _source_file,
            row_number() over (
                partition by source_system, course_id
                order by _file_modified_at desc, retrieved_at desc, _source_file desc
            ) as file_rank
        from course_files
    ),
    source as (
        select raw_source.*
        from raw_source
        inner join
            ranked_course_files
            on raw_source.source_system = ranked_course_files.source_system
            and raw_source.course_id = ranked_course_files.course_id
            and raw_source._source_file = ranked_course_files._source_file
        where ranked_course_files.file_rank = 1
    )

    {{
        deduplicate_raw_table(
            raw_table="raw__openedx__s3__course_xml_blocks",
            partition_columns="source_system, course_id, block_id, block_type",
        )
    }},
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
        from most_recent_source
    )

select *
from cleaned
