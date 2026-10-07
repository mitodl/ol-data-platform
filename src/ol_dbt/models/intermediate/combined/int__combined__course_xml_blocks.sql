{#
  The columns are named because the two staging models are not column-for-column
  the same (the Open edX one also carries coursestructure_xml_archive_version), and
  a `select *` union fails as soon as one side gains a column.
#}

with
    edxorg_course_xml_blocks as (select * from {{ ref('stg__edxorg__s3__course_xml_blocks') }})

    , openedx_course_xml_blocks as (select * from {{ ref('stg__openedx__s3__course_xml_blocks') }})

    , combined as (
        select
            courserun_readable_id
            , coursestructure_xml_source_system
            , coursestructure_xml_block_id
            , coursestructure_xml_block_type
            , coursestructure_xml_block_display_name
            , coursestructure_xml_block_attributes
            , coursestructure_xml_block_path
            , coursestructure_xml_raw_xml
            , video_edx_id
            , video_duration
            , problem_max_attempts
            , problem_weight
            , problem_markdown
            , coursestructure_xml_retrieved_at
        from edxorg_course_xml_blocks
        union all
        select
            courserun_readable_id
            , coursestructure_xml_source_system
            , coursestructure_xml_block_id
            , coursestructure_xml_block_type
            , coursestructure_xml_block_display_name
            , coursestructure_xml_block_attributes
            , coursestructure_xml_block_path
            , coursestructure_xml_raw_xml
            , video_edx_id
            , video_duration
            , problem_max_attempts
            , problem_weight
            , problem_markdown
            , coursestructure_xml_retrieved_at
        from openedx_course_xml_blocks
    )

select *
from combined
