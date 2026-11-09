{#
  integrations__learn__canvas_content_files
  One row per ContentFile of a Canvas course's run, for the Sync task that takes
  over from ingest_canvas_course. Contract:
  docs/design/contentfile_scoped_pull_contract.md §9; the scope key is
  (etl_source, run_readable_id).

  Built on stg__canvas__s3__course_content. A file Tika found no text in is left
  out, as process_olx_path leaves it out. One that failed keeps its row, with
  null content, so a scoped prune does not unpublish it (Learn's failed_keys). The
  ContentFile fields Learn's Canvas ETL leaves unset (description, file_type,
  content_author, content_language, image_src, uid) are null here too.
  edx_module_id equals key, as in Learn. checksum is the MD5 of content, which is
  what Learn's ContentFile.save stores.
#}
with
    content_files as (
        select *
        from {{ ref("stg__canvas__s3__course_content") }}
        where coursecontent_kind = 'file' and coursecontent_text_extraction_status in ('extracted', 'failed')
    )

select
    'canvas' as etl_source,
    courserun_readable_id as run_readable_id,
    coursecontent_key as "key",
    coursecontent_key as edx_module_id,
    coursecontent_title as title,
    cast(null as varchar) as description,
    coursecontent_url as url,
    cast(null as varchar) as file_type,
    coursecontent_text as content,
    coursecontent_content_title as content_title,
    cast(null as varchar) as content_author,
    cast(null as varchar) as content_language,
    'file' as content_type,
    cast(null as varchar) as image_src,
    cast(null as varchar) as uid,
    coursecontent_source_path as source_path,
    coursecontent_file_extension as file_extension,
    case when coursecontent_text is not null then {{ md5_hex("coursecontent_text") }} end as checksum,
    coursecontent_text_extraction_status as extraction_status,
    true as published,
    coursecontent_retrieved_on as last_modified
from content_files
