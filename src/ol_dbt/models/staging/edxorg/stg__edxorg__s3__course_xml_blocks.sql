with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__edxorg__s3__course_xml_blocks") }})

    {{ deduplicate_raw_table(order_by="_airbyte_extracted_at", partition_columns="course_id, block_id, block_type") }},
    cleaned as (
        select
            course_id as courserun_readable_id,
            block_id as coursestructure_xml_block_id,
            block_type as coursestructure_xml_block_type,
            block_display_name as coursestructure_xml_block_display_name,
            xml_attributes as coursestructure_xml_block_attributes,
            xml_path as coursestructure_xml_block_path,
            edx_video_id as video_edx_id,
            cast(duration as decimal(38, 4)) as video_duration,
            max_attempts as problem_max_attempts,
            weight as problem_weight,
            markdown as problem_markdown,
            {{ cast_timestamp_to_iso8601("retrieved_at") }} as coursestructure_xml_retrieved_at
        from most_recent_source
    )

select *
from cleaned
