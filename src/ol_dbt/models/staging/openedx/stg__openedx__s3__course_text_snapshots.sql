{#
  The static asset bundle each Open edX course run's newest document and
  transcript snapshots were extracted from. The text staging models drop an
  empty snapshot's marker row, which is the only row that names its bundle, so
  this reads the raw tables itself. static_assets_version is newer than either
  raw table: dlt adds the column on the first load of a file written after the
  assets began recording it.
#}
{% set raw_documents = source('ol_warehouse_raw_data', 'raw__openedx__s3__course_document_text') %}
{% set raw_transcripts = source('ol_warehouse_raw_data', 'raw__openedx__s3__course_transcript_text') %}
{% set snapshots = [
    ('document', raw_documents, relation_has_column(raw_documents, 'static_assets_version')),
    ('transcript', raw_transcripts, relation_has_column(raw_transcripts, 'static_assets_version')),
] %}

{% for text_kind, relation, versioned in snapshots %}
    {% if not loop.first %}union all{% endif %}
    select distinct
        course_id as courserun_readable_id
        , source_system as content_file_source_system
        , '{{ text_kind }}' as content_file_text_kind
        , {{ 'static_assets_version' if versioned else 'cast(null as varchar)' }}
        as content_file_static_assets_version
    from ({{ newest_course_file_rows(relation, extracted_at_column='extracted_at') }}) as newest_rows
{% endfor %}
