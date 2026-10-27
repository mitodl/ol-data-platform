{# static_references is newer than the raw table: dlt adds the column on the first load
   of a file the document text asset wrote after it began recording links. #}
{% set raw_documents = source('ol_warehouse_raw_data', 'raw__openedx__s3__course_document_text') %}
{% set static_references_exist = relation_has_column(raw_documents, 'static_references') %}

with source as (
    {{ newest_course_file_rows(raw_documents, extracted_at_column='extracted_at') }}
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
    , {{ 'static_references' if static_references_exist else 'cast(null as varchar)' }} as content_file_static_references
from source
-- An empty snapshot loads as one marker row so it can become the newest file;
-- the marker itself is not a file.
where file_path is not null
