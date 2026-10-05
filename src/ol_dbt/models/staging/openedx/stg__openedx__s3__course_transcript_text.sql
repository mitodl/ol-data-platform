with source as (
    {{ newest_course_file_rows(
        source('ol_warehouse_raw_data', 'raw__openedx__s3__course_transcript_text'), extracted_at_column='extracted_at'
    ) }}
)

select
    course_id                               as courserun_readable_id
    , source_system                         as content_file_source_system
    , file_path                             as content_file_path
    -- A failed extraction row has no file_extension; Path.suffix of the path.
    , coalesce(
        file_extension, lower({{ regexp_extract_or_null('file_path', "'\\.[^./]*$'") }})
    )                                       as content_file_extension
    , content_type                          as content_file_mime_type
    , cast(size_bytes as bigint)            as content_file_size_bytes
    , content                               as content_file_text
    , extraction_status                     as content_file_extraction_status
    , _file_modified_at                     as content_file_extracted_at
from source
-- An empty snapshot loads as one marker row so it can become the newest file;
-- the marker itself is not a file.
where file_path is not null
