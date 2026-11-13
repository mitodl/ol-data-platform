{#
  One row per file of an MITx Online or xPRO course run's export that has text
  to read: every OLX block file from course_xml_blocks, and every document and
  transcript the openedx location extracts from the course's static files. A
  file whose extraction failed or found no text keeps its row, with
  extraction_status saying so.

  Each file carries what MIT Learn's ContentFile needs, derived the way Learn's
  process_olx_path does (learning_resources/etl/utils.py): its key
  (edx_module_id), its title, and for a transcript the video it belongs to. Two
  files can share a key (foo bar.pdf and foo_bar.pdf); both are here, flagged
  by shares_edx_module_id. Which files Learn loads, and which of two that share
  a key it keeps, is integrations__learn__content_files' business.

  is_excluded and exclusion_reason are Learn's excluded_olx_paths (staff-only
  subtrees, unreachable tabs and about pages, course settings, asset manifests,
  static files nothing learners see references), from
  stg__openedx__s3__course_file_exclusions, which runs a port of those rules
  over each export. They are known only for a run whose newest blocks file has
  been through it: for any other run (no exclusion rows, or rows from an older
  export) is_exclusion_checked is false and the two are null, and so are
  release_date and the key the exclusions asset gives.

  release_date is the date Learn's OlxArchive.release_dates gives the file, from
  the OLX start dates on its path down from the course, as the exclusions asset
  computes it.

  The course root files course.xml and course/<run>.xml come from
  course_xml_blocks like any other block, and only for an export parsed after
  that asset started carrying them (2026-10). A run whose blocks file is older
  has every other file but not those two until its course_xml_blocks partition
  is rematerialized.
#}

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
        -- Learn's text for an XML file is what Tika returns for it, and Tika goes
        -- by the bytes, not by the XML Content-Type Learn sends. It reads a file
        -- with an <html> element in it as HTML and returns its text nodes (empty
        -- for an OLX pointer such as <html url_name=.../>, which is why Learn has
        -- none of them), and any other XML file as plain text, unchanged. So a
        -- block's content is its XML as written. raw_xml is ElementTree's
        -- re-serialization of it, which writes an empty element as "<a />" and a
        -- tab in an attribute as "&#09;" where the export has "<a/>" and "&#9;";
        -- undoing the two makes the text Learn's for every non-HTML XML file in
        -- the three runs compared on 2026-10-19 (md5 equal to Learn's checksum).
        -- What ElementTree does not keep (comments, the XML declaration, CDATA
        -- markers) still differs from Learn's text where a block has it.
        -- The HTML text is an approximation: CDATA markers go first so a script
        -- body inside one is not cut at its first ">".
        , trim(case
            when has_html
                then {{ regexp_replace_all(
                    html_unescape(
                        regexp_replace_all(
                            "replace(replace(coursestructure_xml_raw_xml, '<![CDATA[', ' '), ']]>', ' ')"
                            , "'<[^>]*>'", "' '"
                        )
                    )
                    , "'\\s+'", "' '"
                ) }}
            else replace(replace(coursestructure_xml_raw_xml, ' />', '/>'), '&#09;', '&#9;')
        end) as content
        , 'extracted' as extraction_status
        , case when not has_html then coursestructure_xml_block_display_name end as xml_display_name
        , cast(coursestructure_xml_retrieved_at as varchar) as extracted_at
    from (
        select
            *
            , {{ regexp_like('coursestructure_xml_raw_xml', "'<html[\\s>/]'") }} as has_html
        from blocks
    ) as blocks_with_html
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
        , {{ format_timestamp_as_iso8601('content_file_extracted_at') }} as extracted_at
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
        , {{ format_timestamp_as_iso8601('content_file_extracted_at') }} as extracted_at
    from transcripts
)

, candidate_files as (
    select * from block_files
    union all
    select * from static_files
)

-- The exclusions asset's rows for the export the run's blocks came from.
-- Exclusions from an older export would miss a block hidden since.
, file_exclusions as (
    select file_exclusions.*
    from {{ ref('stg__openedx__s3__course_file_exclusions') }} as file_exclusions
    inner join (
        select distinct
            courserun_readable_id
            , coursestructure_xml_source_system as source_system
            , coursestructure_xml_archive_version as course_xml_version
        from blocks
    ) as block_versions
        on file_exclusions.courserun_readable_id = block_versions.courserun_readable_id
        and file_exclusions.content_file_source_system = block_versions.source_system
        and file_exclusions.content_file_course_xml_version = block_versions.course_xml_version
)

, checked_runs as (
    select distinct
        courserun_readable_id
        , content_file_source_system as source_system
    from file_exclusions
)

-- The exclusion rows are keyed by the path below the export's root directory,
-- which block paths still carry. A file of a checked run with no row of its
-- own is not excluded.
, checked_files as (
    select
        candidate_files.*
        , checked_runs.courserun_readable_id is not null as is_exclusion_checked
        , case
            when checked_runs.courserun_readable_id is not null
                then coalesce(file_exclusions.content_file_is_excluded, false)
        end as is_excluded
        , file_exclusions.content_file_exclusion_reason as exclusion_reason
        , file_exclusions.content_file_release_date as release_date
        , file_exclusions.content_file_key as exclusions_key
    from candidate_files
    left join checked_runs
        on candidate_files.courserun_readable_id = checked_runs.courserun_readable_id
        and candidate_files.source_system = checked_runs.source_system
    left join file_exclusions
        on candidate_files.courserun_readable_id = file_exclusions.courserun_readable_id
        and candidate_files.source_system = file_exclusions.content_file_source_system
        and {{ regexp_replace_all('candidate_files.source_path', "'^[^/]*/'", "''") }} = file_exclusions.content_file_path
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
    from checked_files
)

, module_files as (
    select
        *
        -- Learn's OlxArchive.key, as the exclusions asset computed it. A block
        -- stored below its type folder (url_name a:b in html/a/b.xml) is keyed
        -- by its url_name, which the path alone can't always spell (a::b). The
        -- path rule below is get_edx_module_id, the same key for every other
        -- file, and covers runs whose exclusion rows predate the key.
        , coalesce(exclusions_key, case
            when folder = 'static'
                then concat('asset-v1:', run_key, '+type@asset+block@', file_name)
            else concat(
                'block-v1:', run_key, '+type@', folder, '+block@'
                , {{ regexp_replace_all('file_name', "'\\.[^.]*$'", "''") }}
            )
        end) as edx_module_id
        , {{ regexp_replace_all('original_file_name', "'\\.[^.]*$'", "''") }} as original_stem
    from keyed_files
)

-- get_video_metadata: every transcript a video/*.xml block names, mapped to that
-- video's id and display name. The id is the url_name on the video file's root,
-- else the url_name the file's path spells: Open edX exports url_name a:b to
-- video/a/b.xml (xmodule name_to_pathname). Learn maps nothing for a root without
-- a url_name; no production video file lacked one on 2026-11-10.
, video_transcripts as (
    select
        blocks.courserun_readable_id
        , blocks.coursestructure_xml_source_system as source_system
        , concat(
            'asset-v1:', replace(blocks.courserun_readable_id, 'course-v1:', '')
            , '+type@asset+block@'
            , replace({{ html_unescape('transcript.src') }}, ' ', '_')
        ) as transcript_module_id
        , coalesce(
            nullif({{ json_query_string('blocks.coursestructure_xml_block_attributes', "'$.url_name'") }}, '')
            , replace(
                {{ regexp_replace_all(
                    regexp_replace_all('blocks.coursestructure_xml_block_path', "'^course/video/'", "''")
                    , "'\\.[^.]*$'", "''"
                ) }}
                , '/', ':'
            )
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
        , source_system
        , transcript_module_id
        , min(video_id) as video_id
        , min(video_title) as video_title
    from video_transcripts
    group by courserun_readable_id, source_system, transcript_module_id
)

, html_titles as (
    select
        courserun_readable_id
        , coursestructure_xml_source_system as source_system
        , coursestructure_xml_block_path as xml_path
        , coursestructure_xml_block_display_name as display_name
    from blocks
    where coursestructure_xml_block_type = 'html'
)

select
    module_files.courserun_readable_id
    , module_files.source_system
    , module_files.source_path
    , module_files.file_extension
    , module_files.edx_module_id
    , count(*) over (
        partition by module_files.source_system, module_files.courserun_readable_id, module_files.edx_module_id
    ) > 1 as shares_edx_module_id
    -- get_title_for_content: a transcript takes its video's name, an XML
    -- block its display_name, an HTML file the display_name of its html/
    -- pointer, and anything else a title made from the file name. Learn
    -- looks for the pointer beside the file first, but under a path that
    -- keeps the export's top folder, which never exists, so a body below
    -- html/ (html/a/b.html) is titled from html/b.xml too.
    , coalesce(
        nullif(case
            when module_files.file_extension = '.xml' then module_files.xml_display_name
            when module_files.file_extension = '.html' then html_titles.display_name
            else video_transcript_map.video_title
        end, '')
        , {{ title_case("replace(replace(module_files.original_stem, '_', ' '), '-', ' ')") }}
    ) as title
    , video_transcript_map.video_id as video_url_name
    , module_files.content
    , module_files.extraction_status
    , module_files.extracted_at
    , module_files.release_date
    , module_files.is_exclusion_checked
    , module_files.is_excluded
    , module_files.exclusion_reason
from module_files
left join video_transcript_map
    on module_files.courserun_readable_id = video_transcript_map.courserun_readable_id
    and module_files.source_system = video_transcript_map.source_system
    and module_files.edx_module_id = video_transcript_map.transcript_module_id
left join html_titles
    on module_files.file_extension = '.html'
    and module_files.courserun_readable_id = html_titles.courserun_readable_id
    and module_files.source_system = html_titles.source_system
    and html_titles.xml_path = concat('course/html/', module_files.original_stem, '.xml')
