-- The two staging models are unioned by position, so each side names its
-- columns. A column only one of them has (the openedx side carries
-- coursestructure_xml_archive_version) would otherwise fail the union.
{% set block_columns = [
    "courserun_readable_id",
    "coursestructure_xml_source_system",
    "coursestructure_xml_block_id",
    "coursestructure_xml_block_type",
    "coursestructure_xml_block_display_name",
    "coursestructure_xml_block_attributes",
    "coursestructure_xml_block_path",
    "coursestructure_xml_raw_xml",
    "video_edx_id",
    "video_duration",
    "problem_max_attempts",
    "problem_weight",
    "problem_markdown",
    "coursestructure_xml_retrieved_at",
] %}

with
    edxorg_course_xml_blocks as (
        select {{ block_columns | join(", ") }} from {{ ref('stg__edxorg__s3__course_xml_blocks') }}
    )

    , openedx_course_xml_blocks as (
        select {{ block_columns | join(", ") }} from {{ ref('stg__openedx__s3__course_xml_blocks') }}
    )

    , combined as (
        select * from edxorg_course_xml_blocks
        union all
        select * from openedx_course_xml_blocks
    )

select *
from combined
