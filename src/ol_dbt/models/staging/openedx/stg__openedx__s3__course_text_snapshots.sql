{#
  The rows of each Open edX course run's newest document and transcript
  snapshots, read once so the files stg__openedx__s3__course_document_text and
  stg__openedx__s3__course_transcript_text give and the bundle version
  int__openedx__content_files checks them by come from the same pick of newest
  file. Each read of the raw tables picks again, and a load landing between two
  reads would pair one snapshot's version with another's files.

  An empty snapshot is one marker row with no file path, kept here because it is
  the only row that names its bundle. Columns newer than a raw table (dlt adds
  them on the first load of a file that has them) read as null until then.
#}
{% set snapshots = [
    ('document', source('ol_warehouse_raw_data', 'raw__openedx__s3__course_document_text')),
    ('transcript', source('ol_warehouse_raw_data', 'raw__openedx__s3__course_transcript_text')),
] %}

{% for text_kind, raw_snapshots in snapshots %}
    {% if not loop.first %}union all{% endif %}
    select
        course_id                               as courserun_readable_id
        , source_system                         as content_file_source_system
        , '{{ text_kind }}'                     as content_file_text_kind
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
        , {{ 'static_references' if relation_has_column(raw_snapshots, 'static_references')
            else 'cast(null as varchar)' }}     as content_file_static_references
        , {{ 'static_assets_version' if relation_has_column(raw_snapshots, 'static_assets_version')
            else 'cast(null as varchar)' }}     as content_file_static_assets_version
    from ({{ newest_course_file_rows(raw_snapshots, extracted_at_column='extracted_at') }}) as newest_rows
{% endfor %}
