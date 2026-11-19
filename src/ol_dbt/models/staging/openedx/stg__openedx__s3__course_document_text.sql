{# The document rows of stg__openedx__s3__course_text_snapshots, which picks each
   course's newest file. #}
select
    courserun_readable_id
    , content_file_source_system
    , content_file_path
    , content_file_extension
    , content_file_mime_type
    , content_file_size_bytes
    , content_file_text
    , content_file_extraction_status
    , content_file_extracted_at
    , content_file_static_references
    , content_file_static_assets_version
from {{ ref('stg__openedx__s3__course_text_snapshots') }}
where
    content_file_text_kind = 'document'
    -- An empty snapshot loads as one marker row so it can become the newest file;
    -- the marker itself is not a file.
    and content_file_path is not null
