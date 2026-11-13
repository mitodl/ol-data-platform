{%- set run_buckets = 48 -%}
{{ config(
    partition_by=(
        ['etl_source, bucket(run_readable_id, ' ~ run_buckets ~ ')']
        if target.type == 'starrocks' else none
    ),
    properties=(
        none if target.type == 'starrocks'
        else {
            "format": "'PARQUET'",
            "partitioning": "ARRAY['etl_source', 'bucket(run_readable_id, " ~ run_buckets ~ ")']",
        }
    ),
) }}

{#
  integrations__learn__content_files
  One row per ContentFile of an MITx Online or xPRO course run, for MIT Learn's
  SyncOpenEdXContentFilesTask. Contract: docs/design/contentfile_scoped_pull_contract.md
  §9; the scope key is (etl_source, run_readable_id).

  Built on int__openedx__content_files, which has every file of a run's export
  with its Learn key, title and exclusion flags. Learn's own rules for which of
  them become ContentFiles are applied here, as Learn's process_olx_path and
  documents_from_olx apply them (learning_resources/etl/utils.py):
  - only runs whose export has been through the exclusions check, less the files
    excluded_olx_paths drops. Without the check a run's files would include ones
    Learn hides, and a scoped pull would publish them;
  - only Learn's text file types, nothing under a draft directory, and not the
    asset manifests or the announcement archive;
  - no file whose text is empty. A failed extraction keeps its row, with null
    content, so a scoped prune does not unpublish it;
  - one row per (etl_source, run_readable_id, key): Learn upserts on key, so of
    two files that map to one key only one survives. A file with text wins, then
    a file that is not XML, then the first path. Like Learn's key_dates, the row
    takes the latest release_date of every file of its key, empty or not;
  - url is get_url_from_module_id's, on the LMS of the run's source.

  The ContentFile fields Learn's Open edX ETL leaves unset (description, file_type, content_author, content_language,
  image_src, uid) are null here too. content_title is always '', and so is
  Learn's: its _extract_content_with_tika reads Tika's metadata "title", which
  Learn's production content files never have, even for PDFs with an embedded
  /Title (2026-10-02, three sample runs, 64 PDF/PPTX files). Learn's title then
  falls back to the file name, as title does here. checksum is the MD5 of the extracted text, not of the file
  bytes as Learn's archive_checksum is, so it changes when the text does.
  release_date is ContentFile.release_date (mit-learn#4093). Learn shows a file
  only while it is published and its release_date is null or past, so a
  future-dated row is here but hidden in Learn until that date.

  Partitioned so a read of one run opens one bucket of files: the ml chunks asset
  reads one (etl_source, run_readable_id) at a time, as the contract's scoped pull
  will. sorted_by would not do it: it orders rows within each file, not across
  files. Two sources by 48 buckets stays under Trino's default limit of 100
  partitions per writer. A table model is replaced whole on every build, so
  changing the spec needs no --full-refresh.
#}

{% set valid_text_file_types = [
    '.doc', '.docx', '.htm', '.html', '.json', '.md', '.pdf', '.tex', '.ppt',
    '.pptx', '.rtf', '.sjson', '.srt', '.txt', '.vtt', '.xml'
] %}
{% set non_content_olx_files = [
    'course/policies/assets.json', 'course/assets/assets.xml', 'course/info/updates.items.json'
] %}

with olx_documents as (
    select * from {{ ref('int__openedx__content_files') }}
    where
        is_exclusion_checked
        and not is_excluded
        and file_extension in ('{{ valid_text_file_types | join("', '") }}')
        and source_path not in ('{{ non_content_olx_files | join("', '") }}')
        and not {{ regexp_like("source_path", "'draft.*/'") }}
)

, dated_files as (
    select
        *
        -- ISO 8601 UTC strings of one fixed width, so the latest sorts last
        , max(release_date) over (
            partition by source_system, courserun_readable_id, edx_module_id
        ) as key_release_date
    from olx_documents
)

, content_files as (
    select
        *
        , {{ openedx_lms_root_url('source_system') }} as root_url
        , row_number() over (
            partition by source_system, courserun_readable_id, edx_module_id
            order by
                case when extraction_status = 'failed' then 1 else 0 end nulls last
                , case when file_extension = '.xml' then 1 else 0 end nulls last
                , source_path nulls last
        ) as key_rank
    from dated_files
    where extraction_status = 'failed' or trim(coalesce(content, '')) != ''
)

select
    source_system as etl_source
    , courserun_readable_id as run_readable_id
    , edx_module_id as "key"
    , edx_module_id
    , title
    , cast(null as varchar) as description
    -- get_url_from_module_id
    , case
        when edx_module_id like 'asset%' and video_url_name is not null
            then concat(root_url, '/courses/', courserun_readable_id, '/jump_to_id/', video_url_name)
        when edx_module_id like 'asset%'
            then concat(root_url, '/', edx_module_id)
        when {{ regexp_like(
            regexp_replace_all(
                element_at_array("split(edx_module_id, '@')", array_length("split(edx_module_id, '@')"))
                , "'[{}-]'", "''"
            )
            , "'^[0-9a-fA-F]{32}$'"
        ) }}
            then concat(
                root_url, '/courses/', courserun_readable_id, '/jump_to_id/'
                , {{ element_at_array("split(edx_module_id, '@')", array_length("split(edx_module_id, '@')")) }}
            )
    end as url
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
    , key_release_date as release_date
from content_files
where key_rank = 1
