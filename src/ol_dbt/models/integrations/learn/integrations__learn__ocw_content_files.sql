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

  Learn makes the key it reads by splitting a path that does not start with
  "courses" on "courses" and keeping what lies between the first two, so a
  file whose name holds "courses" is cut short. The platform reads the whole
  path. Learn reads the cut key only when its extension is one it extracts
  (VALID_FILE_TYPES, Path(key).suffix in learning_resources/etl/ocw.py). A cut
  key ends inside the real path, so it names no file: with an extension Learn
  reads, the file is missing and Learn drops the resource; without one, Learn
  keeps the resource with no text (the one production case on 2026-11-20, a
  24.908 PDF named ..._Discourses.pdf).
#}

{% set learn_text_extensions = [
    '.csv', '.doc', '.docx', '.htm', '.html', '.json', '.m', '.mat', '.md', '.pdf', '.ppt',
    '.pptx', '.ps', '.py', '.r', '.rtf', '.sjson', '.srt', '.txt', '.vtt', '.xls', '.xlsx', '.xml',
] %}

with int_content_files as (
    select
        *
        , coalesce(
            text_file_path not like 'courses%'
            and length(text_file_path) - length(replace(text_file_path, 'courses', '')) >= 2 * length('courses')
            , false
        ) as learn_cuts_path
        -- What Learn's cut key holds after "courses", up to the next "courses".
        , {{ regexp_extract_or_null('text_file_path', "'courses(.*?)courses'", 1) }} as learn_cut_rest
    from {{ ref('int__ocw__content_files') }}
)

, learn_cut_keys as (
    select
        *
        -- Path.suffix of the cut key: its last segment's extension, trailing
        -- slashes ignored.
        , lower(
            {{ regexp_extract_or_null(regexp_replace_all('learn_cut_rest', "'/+$'", "''"), "'\\.[^./]+$'") }}
        ) in ('{{ learn_text_extensions | join("', '") }}') as learn_reads_cut_key
    from int_content_files
)

, content_files as (
    select
        courserun_readable_id
        , content_file_key
        , title
        , description
        , url
        , file_type
        , case when not learn_cuts_path then content end as content
        , content_type
        , image_src
        , file_extension
        , case
            when not learn_cuts_path then extraction_status
            when learn_reads_cut_key then 'missing'
        end as extraction_status
        , retrieved_on
        , content_tags
        , youtube_id
        , has_text_file
        , is_3play_artifact
    from learn_cut_keys
)

, learn_content_files as (
    select * from content_files
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
from learn_content_files
