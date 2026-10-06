with source as (
    {{ newest_course_file_rows(
        source('ol_warehouse_raw_data', 'raw__openedx__s3__course_file_exclusions'), extracted_at_column='extracted_at'
    ) }}
)

select
    course_id                               as courserun_readable_id
    , source_system                         as content_file_source_system
    , course_xml_version                    as content_file_course_xml_version
    , file_path                             as content_file_path
    , excluded = 'true'                     as content_file_is_excluded
    , exclusion_reason                      as content_file_exclusion_reason
    , _file_modified_at                     as content_file_checked_at
from source
