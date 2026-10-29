{# release_date is newer than the raw table: dlt adds the column on the first load
   after the exclusions asset began dating files. #}
{% set raw_exclusions = source('ol_warehouse_raw_data', 'raw__openedx__s3__course_file_exclusions') %}
{% set release_date_exists = not execute
    or 'release_date' in adapter.get_columns_in_relation(raw_exclusions) | map(attribute='name') | map('lower') %}

with source as (
    {{ newest_course_file_rows(raw_exclusions, extracted_at_column='extracted_at') }}
)

select
    course_id                               as courserun_readable_id
    , source_system                         as content_file_source_system
    , course_xml_version                    as content_file_course_xml_version
    , file_path                             as content_file_path
    , excluded = 'true'                     as content_file_is_excluded
    , exclusion_reason                      as content_file_exclusion_reason
    , {{ 'release_date' if release_date_exists else 'cast(null as varchar)' }} as content_file_release_date
    , _file_modified_at                     as content_file_checked_at
from source
