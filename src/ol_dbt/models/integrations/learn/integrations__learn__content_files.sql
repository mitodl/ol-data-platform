{#
  integrations__learn__content_files
  One row per ContentFile of an MITx Online or xPRO course run, for MIT Learn's
  SyncOpenEdXContentFilesTask. Contract: docs/design/contentfile_scoped_pull_contract.md
  §9; the scope key is (etl_source, run_readable_id).

  Built on int__openedx__content_files. The ContentFile fields Learn's Open edX
  ETL leaves unset (description, file_type, content_author, content_language,
  image_src, uid) are null here too. content_title is always '', and so is
  Learn's: its _extract_content_with_tika reads Tika's metadata "title", which
  Learn's production content files never have, even for PDFs with an embedded
  /Title (2026-10-02, three sample runs, 64 PDF/PPTX files). Learn's title then
  falls back to the file name, as title does here. checksum is the MD5 of the extracted text, not of the file
  bytes as Learn's archive_checksum is, so it changes when the text does.
#}

with content_files as (
    select * from {{ ref('int__openedx__content_files') }}
)

select
    source_system as etl_source
    , courserun_readable_id as run_readable_id
    , edx_module_id as "key"
    , edx_module_id
    , title
    , cast(null as varchar) as description
    , url
    , cast(null as varchar) as file_type
    , content
    , '' as content_title
    , cast(null as varchar) as content_author
    , cast(null as varchar) as content_language
    , 'file' as content_type
    , cast(null as varchar) as image_src
    , cast(null as varchar) as uid
    , source_path
    , file_extension
    , case when content is not null then {{ md5_hex('content') }} end as checksum
    , extraction_status
    , true as published
    , extracted_at as last_modified
from content_files
