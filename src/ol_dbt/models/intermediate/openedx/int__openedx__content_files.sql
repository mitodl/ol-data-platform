{#
  One row per file of an MITx Online or xPRO course run that MIT Learn would load
  as a ContentFile, derived the way Learn's process_olx_path does
  (learning_resources/etl/utils.py): OLX block XML from course_xml_blocks, and the
  document and transcript text the openedx location extracts from the course's
  static files.

  Learn skips any file whose extracted text is empty, which is why OLX pointer
  files (<html filename=.../>) and text-less verticals never reach it; the same
  rule applies here. Block text approximates Tika's output for XML by stripping
  tags. A file whose extraction failed keeps its row, with null content and
  extraction_status "failed", so a consumer can tell it apart from a removed file.

  Not reproduced yet (tracked separately): Learn's exclusion of staff-only
  subtrees and of static files nothing in the course references
  (excluded_olx_paths), Tika's metadata title for documents, and the course root
  files course.xml and course/<run>.xml, which course_xml_blocks does not carry.
#}

{% set valid_text_file_types = [
    '.doc', '.docx', '.htm', '.html', '.json', '.md', '.pdf', '.tex', '.ppt',
    '.pptx', '.rtf', '.sjson', '.srt', '.txt', '.vtt', '.xml'
] %}
{% set non_content_olx_files = [
    'course/policies/assets.json', 'course/assets/assets.xml', 'course/info/updates.items.json'
] %}

with blocks as (
    select * from {{ ref('stg__openedx__s3__course_xml_blocks') }}
    where coursestructure_xml_source_system in ('mitxonline', 'xpro')
)

, documents as (
    select * from {{ ref('stg__openedx__s3__course_document_text') }}
    where content_file_source_system in ('mitxonline', 'xpro')
)

, transcripts as (
    select * from {{ ref('stg__openedx__s3__course_transcript_text') }}
    where content_file_source_system in ('mitxonline', 'xpro')
)

, block_files as (
    select
        courserun_readable_id
        , coursestructure_xml_source_system as source_system
        , coursestructure_xml_block_path as source_path
        , '.xml' as file_extension
        -- An approximation of Tika's text for XML, which carries attribute values
        -- as well as text nodes; that is why Learn keeps every chapter and
        -- vertical, whose XML has no text of its own. CDATA markers go first so a
        -- script body inside one is not cut at its first ">".
        , trim({{ regexp_replace_all(
            html_unescape(
                "concat("
                ~ array_join("regexp_extract_all(coursestructure_xml_raw_xml, '=\"([^\"]*)\"', 1)", ' ')
                ~ ", ' ', "
                ~ regexp_replace_all(
                    "replace(replace(coursestructure_xml_raw_xml, '<![CDATA[', ' '), ']]>', ' ')"
                    , "'<[^>]*>'", "' '"
                )
                ~ ")"
            )
            , "'\\s+'", "' '"
        ) }}) as content
        , 'extracted' as extraction_status
        , coursestructure_xml_block_display_name as xml_display_name
        , cast(coursestructure_xml_retrieved_at as varchar) as extracted_at
    from blocks
)

, static_files as (
    select
        courserun_readable_id
        , content_file_source_system as source_system
        , concat('course/', content_file_path) as source_path
        , lower(content_file_extension) as file_extension
        , content_file_text as content
        , content_file_extraction_status as extraction_status
        , cast(null as varchar) as xml_display_name
        , {{ cast_timestamp_to_iso8601('content_file_extracted_at') }} as extracted_at
    from documents
    union all
    select
        courserun_readable_id
        , content_file_source_system as source_system
        , concat('course/', content_file_path) as source_path
        , lower(content_file_extension) as file_extension
        , content_file_text as content
        , content_file_extraction_status as extraction_status
        , cast(null as varchar) as xml_display_name
        , {{ cast_timestamp_to_iso8601('content_file_extracted_at') }} as extracted_at
    from transcripts
)

, candidate_files as (
    select * from block_files
    union all
    select * from static_files
)

, keyed_files as (
    select
        *
        -- get_edx_module_id: spaces become underscores, the block type is the
        -- file's directory, and a static file keeps its whole name.
        , {{ element_at_array("split(replace(source_path, ' ', '_'), '/')",
            array_length("split(replace(source_path, ' ', '_'), '/')") ~ " - 1") }} as folder
        , {{ element_at_array("split(replace(source_path, ' ', '_'), '/')",
            array_length("split(replace(source_path, ' ', '_'), '/')")) }} as file_name
        , {{ element_at_array("split(source_path, '/')", array_length("split(source_path, '/')")) }}
            as original_file_name
        , replace(courserun_readable_id, 'course-v1:', '') as run_key
    from candidate_files
    -- documents_from_olx: Learn's text file types, nothing under a draft
    -- directory, and not the asset manifests or the announcement archive.
    where
        file_extension in ('{{ valid_text_file_types | join("', '") }}')
        and source_path not in ('{{ non_content_olx_files | join("', '") }}')
        and not {{ regexp_like("source_path", "'draft.*/'") }}
)

, module_files as (
    select
        *
        , case
            when folder = 'static'
                then concat('asset-v1:', run_key, '+type@asset+block@', file_name)
            else concat(
                'block-v1:', run_key, '+type@', folder, '+block@'
                , regexp_replace(file_name, '\.[^.]*$', '')
            )
        end as edx_module_id
        , regexp_replace(original_file_name, '\.[^.]*$', '') as original_stem
    from keyed_files
)

-- get_video_metadata: every transcript a video/*.xml block names, mapped to that
-- video's id and display name.
, video_transcripts as (
    select
        blocks.courserun_readable_id
        , concat(
            'asset-v1:', replace(blocks.courserun_readable_id, 'course-v1:', '')
            , '+type@asset+block@'
            , replace({{ html_unescape('transcript.src') }}, ' ', '_')
        ) as transcript_module_id
        , regexp_replace(
            {{ element_at_array("split(blocks.coursestructure_xml_block_path, '/')",
                array_length("split(blocks.coursestructure_xml_block_path, '/')")) }}
            , '\.[^.]*$', ''
        ) as video_id
        , blocks.coursestructure_xml_block_display_name as video_title
    from blocks
    cross join {{ unnest_regexp_matches(
        'blocks.coursestructure_xml_raw_xml', "'<transcript\\s[^>]*src=\"([^\"]*)\"'", 'transcript', 'src'
    ) }}
    where blocks.coursestructure_xml_block_path like 'course/video/%'
)

, video_transcript_map as (
    select
        courserun_readable_id
        , transcript_module_id
        , min(video_id) as video_id
        , min(video_title) as video_title
    from video_transcripts
    group by courserun_readable_id, transcript_module_id
)

, html_titles as (
    select
        courserun_readable_id
        , coursestructure_xml_block_path as xml_path
        , coursestructure_xml_block_display_name as display_name
    from blocks
    where coursestructure_xml_block_type = 'html'
)

, resolved as (
    select
        module_files.courserun_readable_id
        , module_files.source_system
        , module_files.source_path
        , module_files.file_extension
        , module_files.content
        , module_files.extraction_status
        , module_files.extracted_at
        , module_files.edx_module_id
        , case module_files.source_system
            when 'mitxonline' then 'https://courses.mitxonline.mit.edu'
            when 'xpro' then 'https://courses.xpro.mit.edu'
        end as root_url
        , video_transcript_map.video_id
        -- get_title_for_content: a transcript takes its video's name, an XML
        -- block its display_name, an HTML file the display_name of its html/
        -- pointer, and anything else a title made from the file name.
        , coalesce(
            nullif(case
                when module_files.file_extension = '.xml' then module_files.xml_display_name
                when module_files.file_extension = '.html' then html_titles.display_name
                else video_transcript_map.video_title
            end, '')
            , {{ title_case("replace(replace(module_files.original_stem, '_', ' '), '-', ' ')") }}
        ) as title
    from module_files
    left join video_transcript_map
        on module_files.courserun_readable_id = video_transcript_map.courserun_readable_id
        and module_files.edx_module_id = video_transcript_map.transcript_module_id
    left join html_titles
        on module_files.file_extension = '.html'
        and module_files.courserun_readable_id = html_titles.courserun_readable_id
        and html_titles.xml_path = concat('course/html/', module_files.original_stem, '.xml')
    -- Learn skips a file whose text is empty. A failed extraction is kept.
    where module_files.extraction_status = 'failed' or trim(coalesce(module_files.content, '')) != ''
)

, with_urls as (
    select
        *
        -- get_url_from_module_id
        , case
            when edx_module_id like 'asset%' and video_id is not null
                then concat(root_url, '/courses/', courserun_readable_id, '/jump_to_id/', video_id)
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
        -- One ContentFile per (run, key): Learn upserts on key, so of two files
        -- that map to one key only one survives. Prefer a file with text.
        , row_number() over (
            partition by courserun_readable_id, edx_module_id
            order by
                case when extraction_status = 'failed' then 1 else 0 end
                , case when file_extension = '.xml' then 1 else 0 end
                , source_path
        ) as key_rank
    from resolved
)

select
    courserun_readable_id
    , source_system
    , edx_module_id
    , source_path
    , file_extension
    , title
    , url
    , content
    , extraction_status
    , extracted_at
from with_urls
where key_rank = 1
