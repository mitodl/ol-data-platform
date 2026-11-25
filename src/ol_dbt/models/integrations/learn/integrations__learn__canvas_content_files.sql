{#
  integrations__learn__canvas_content_files
  One row per ContentFile of a Canvas course's run, for the Sync task that takes
  over from ingest_canvas_course. Contract:
  docs/design/contentfile_scoped_pull_contract.md §9; the scope key is
  (etl_source, run_readable_id).

  Staging has every member of each course's archive. Learn's rules that need the
  whole archive (who can see a member, its key, title and URL) ran when it was
  read, into the learn_* columns. The rest are applied here:
  - a member Learn treats as published (coursecontent_learn_published), of a type
    Learn extracts text from, not under a folder with "draft" in its path;
  - a PDF pypdf cannot read is a file Learn could not read, whatever Tika made of
    it (Learn raises InvalidPDFError before extracting);
  - Learn's key leaves the extension out, so a.pdf and a.docx in one folder are
    one ContentFile. Learn keeps whichever it loaded last; here, among the files
    with the best outcome (text, then a failure, then unreadable, then no text),
    the last by path.
  A file Tika found no text in is left out, as process_olx_path leaves it out. One
  that failed keeps its row, with null content, so a scoped prune does not
  unpublish it (Learn's failed_keys). The ContentFile fields Learn's Canvas ETL
  leaves unset (description, file_type, content_author, content_language,
  image_src, uid) are null here too. edx_module_id equals key, as in Learn.
  checksum is the MD5 of content, which is what Learn's ContentFile.save stores.
#}
with
    courses as (
        select course_id, course_code
        from {{ ref("stg__canvas__s3__course_content") }}
        where coursecontent_kind = 'course'
    ),
    members as (
        select
            stg.*,
            -- What Learn's loader would make of this read. A key's file is chosen
            -- on it, before an earlier read's text stands in for a failure, as
            -- Learn chooses on what it reads now.
            case
                when stg.coursecontent_file_extension = '.pdf' and stg.coursecontent_pdf_is_valid = false
                then 'unreadable'
                else stg.coursecontent_text_read_status
            end as learn_read_status
        from {{ ref("stg__canvas__s3__course_content") }} as stg
        where
            stg.coursecontent_kind = 'member'
            and stg.coursecontent_learn_published
            and not stg.coursecontent_draft_in_parent_path
            -- VALID_TEXT_FILE_TYPES in mit-learn's learning_resources/constants.py
            and stg.coursecontent_file_extension in (
                '.doc',
                '.docx',
                '.htm',
                '.html',
                '.json',
                '.md',
                '.pdf',
                '.tex',
                '.ppt',
                '.pptx',
                '.rtf',
                '.sjson',
                '.srt',
                '.txt',
                '.vtt',
                '.xml'
            )
    ),
    chosen as (
        select
            members.*,
            row_number() over (
                partition by members.course_id, members.coursecontent_learn_key
                order by
                    case members.learn_read_status
                        when 'extracted' then 1 when 'failed' then 2 when 'unreadable' then 3 else 4
                    end nulls last,
                    members.coursecontent_member_path desc
            ) as key_rank
        from members
    ),
    statuses as (
        select
            chosen.*,
            -- Only a read needs to tell unreadable from failed. Downstream both are
            -- a file with no text whose row keeps a prune from taking it. A failure
            -- takes the text of an earlier read of the same bytes (staging).
            case
                when chosen.learn_read_status = 'unreadable'
                then 'failed'
                else chosen.coursecontent_text_extraction_status
            end as extraction_status
        from chosen
        where chosen.key_rank = 1 and chosen.learn_read_status in ('extracted', 'failed', 'unreadable')
    ),
    content_files as (
        select statuses.*, case when statuses.extraction_status = 'extracted' then statuses.coursecontent_text end as content
        from statuses
    )

select
    'canvas' as etl_source,
    {{ canvas_run_readable_id("content_files.course_id", "courses.course_code") }} as run_readable_id,
    content_files.coursecontent_learn_key as "key",
    content_files.coursecontent_learn_key as edx_module_id,
    content_files.coursecontent_learn_title as title,
    cast(null as varchar) as description,
    content_files.coursecontent_learn_url as url,
    cast(null as varchar) as file_type,
    content_files.content,
    content_files.coursecontent_learn_content_title as content_title,
    cast(null as varchar) as content_author,
    cast(null as varchar) as content_language,
    'file' as content_type,
    cast(null as varchar) as image_src,
    cast(null as varchar) as uid,
    -- ContentFile.source_path, which has a leading / for a file at the root.
    case
        when {{ string_position("'/'", "content_files.coursecontent_member_path") }} = 0
        then concat('/', content_files.coursecontent_member_path)
        else content_files.coursecontent_member_path
    end as source_path,
    content_files.coursecontent_file_extension as file_extension,
    case when content_files.content is not null then {{ md5_hex("content_files.content") }} end as checksum,
    content_files.extraction_status,
    true as published,
    content_files.coursecontent_retrieved_on as last_modified
from content_files
inner join courses on content_files.course_id = courses.course_id
