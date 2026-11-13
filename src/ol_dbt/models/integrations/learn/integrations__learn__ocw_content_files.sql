{#
  integrations__learn__ocw_content_files
  One row per ContentFile of an OCW course run, for MIT Learn's
  SyncOCWContentFilesTask. Contract: docs/design/contentfile_scoped_pull_contract.md
  §9; the scope key is (etl_source, run_readable_id).

  Built on int__ocw__content_files. The ContentFile fields Learn's OCW ETL
  leaves unset (content_author, content_language, uid, edx_module_id,
  source_path) are null here too. content_title is the title, as in Learn.
  checksum is the MD5 of content, null for empty content, which is what Learn's
  ContentFile.save stores.

  Learn's drops, which int__ocw__content_files flags: Learn skips a course
  with neither uid, and keeps a resource only when its text file is a path in
  the bucket (has_text_file, false for a video whose transcript list Learn
  cannot read) and the file is there, and never a 3Play caption or PDF file.
#}

with content_files as (
    select * from {{ ref('int__ocw__content_files') }}
    where
        courserun_readable_id is not null
        and (
            content_type = 'page'
            or (
                has_text_file
                and not is_3play_artifact
                and coalesce(extraction_status, '') != 'missing'
            )
        )
)

select
    'ocw' as etl_source
    , courserun_readable_id as run_readable_id
    , content_file_key as "key"
    , cast(null as varchar) as edx_module_id
    , title
    , description
    , url
    , file_type
    , content
    , title as content_title
    , cast(null as varchar) as content_author
    , cast(null as varchar) as content_language
    , content_type
    , image_src
    , cast(null as varchar) as uid
    , cast(null as varchar) as source_path
    , file_extension
    , case when content != '' then {{ md5_hex('content') }} end as checksum
    , extraction_status
    , true as published
    , retrieved_on as last_modified
    , content_tags
    , youtube_id
from content_files
