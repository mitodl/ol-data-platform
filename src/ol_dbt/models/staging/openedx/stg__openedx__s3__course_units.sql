with source as (
    {{ newest_course_file_rows(
        source('ol_warehouse_raw_data', 'raw__openedx__s3__course_units'), extracted_at_column='extracted_at'
    ) }}
)

select
    course_id                               as courserun_readable_id
    , source_system                         as unit_source_system
    , course_xml_version                    as unit_course_xml_version
    , unit_order
    , external_id                           as unit_external_id
    , kind                                  as unit_kind
    , title                                 as unit_title
    , url_name                              as unit_url_name
    , content_files                         as unit_content_files
    , _file_modified_at                     as unit_built_at
from source
-- A run whose units could not be built lands an empty file, loaded as one marker
-- row so that it becomes the run's newest; the marker itself is not a unit.
where external_id is not null
