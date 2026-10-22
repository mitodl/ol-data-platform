{#
    The text an extractor reads for each scope of an Open edX course run, one row per
    source item in reading order.

    Scopes:
      unit       one per sequential a learner can navigate to (not staff-only, alone or by
                 its chapter, and not hidden from the table of contents), its leaf blocks
                 in course-tree order (dim_course_content)
      context    the about pages, reachable tabs and course info pages, then the blocks of
                 the sequentials that describe the course or a module rather than teach:
                 one titled "Module Overview and Learning Goals" wherever it sits, and a
                 chapter's only sequential when its title starts a word with intro,
                 overview, welcome, conclusion, summary or wrap. These are mit-learn's
                 context-sequential rules (learning_resources/etl/utils.py,
                 _is_context_sequential), so the platform and Learn agree on units.
      materials  the run's static documents (slides, PDFs) that no unit's html links to.
                 A document an html block links to (content_file_static_references) is
                 read right after that block, in its unit or in context.

    Problem text is the question side only (courseware_problem_question_text). A
    library_content block contributes its first problem, since its children are
    randomized variants of one question. Files are dropped by the export's own exclusion
    rules, except about_page: Learn leaves the about pages out of search, but the
    description and prerequisites stated there are what the context scope is for. Each
    about page is one field of the about page, so its text is prefixed with the field's
    name ("Prerequisites" then "None"); bare, "None" says nothing. The description pages
    are left as they are, since the course header already gives them as DESCRIPTION.
    Answer keys and solution sets (courseware_answer_key over the file's path, minus its
    extension, and the opening of its text) are never read.

    The build covers the runs int__courseware__extraction_allowlist approves.
    var('courseware_courseruns') replaces that with a list of runs, for local
    development against production text.

    OCW courses (platform ocw, keyed by Learn's run id) are read from the published
    site, int__ocw__content_files:

      unit       a page that has child pages, with its children (a week of an OCW Scholar
                 course, a part of a unit); a top-level section page (Lecture Notes,
                 Assignments, Exams); a page whose parent page is not published;
                 and, in a flat course, one per session: the
                 resources linked in row N of any table keyed by SES #, LEC # or WEEK #,
                 titled from the calendar, and the problem set its calendar row
                 releases ("PS 3 out") when no session row links it
      context    the course description and the syllabus pages, with what they link
      materials  documents no page links

    A course taught through three or more top-level sections that hold child pages (an
    OCW Scholar course's weeks) does not read its textbook section (Online Textbook,
    Open Textbook): the weeks teach, and the book is reference reading that outweighs
    them (8.01SC's is nine tenths of its text). What only the textbook links is not
    read and does not land in materials.
    A course that is its textbook (Calculus Open Textbook) reads it as units.

    A link to a resource also links what that resource's description links, right
    after it: 18.01SC's pages link video clips whose descriptions link the session
    notes. The description is read from stg__ocw__s3__course_content, since
    int__ocw__content_files drops a video with no transcript file.

    Answer keys are left out: a resource of type Problem Set Solutions or Exam Solutions,
    that courseware_answer_key reads as one from its file name, title and opening text,
    that a table links in a column headed Solutions (or Answers, Key), or whose text is
    the same as such a resource's; a page the same way by its name and title, except one
    that holds problems and solutions together ("Problem Sets and Solutions"), whose
    solution resources go instead. What only a solution page links is not read, and not
    counted as linked for materials. A solution page with child pages keeps them as a
    unit, unread itself. Quiz solutions, correctness icons and answer, hint and sample
    answer reveal blocks are cut from page HTML before its links are read. A document
    linked from more than one scope is read once, in the first unit. A link to another
    course's resource is left to that course: it is not read here and does not count as
    linked there.

    var('courseware_ocw_courses') selects OCW courses by slug. Without it a build reads
    no OCW course, since the allowlist does not cover OCW yet. Setting either selector
    alone builds only what it selects.
#}
{% set courseruns = var('courseware_courseruns', []) %}
{% set ocw_courses = var('courseware_ocw_courses', []) %}
{% set honored_exclusions = ['unreferenced_static', 'unreachable_tab', 'legacy_announcements', 'non_content', 'course_settings'] %}
{% set document_extensions = ['.pdf', '.pptx', '.ppt', '.docx', '.doc', '.txt', '.md'] %}
{% set context_sequential_titles = ['module overview and learning goals'] %}
{# ml.lib.courseware.scopes.DESCRIPTION_PAGES #}
{% set description_pages = ['about/short_description.html', 'about/overview.html', 'about/description.html'] %}

{% if courseruns or ocw_courses %}
    {% set courserun_filter = "('" ~ courseruns | join("', '") ~ "')" if courseruns else "('')" %}
    {% set ocw_course_filter = "('" ~ ocw_courses | join("', '") ~ "')" if ocw_courses else "('')" %}
{% else %}
    {% set courserun_filter = "(select courserun_readable_id from " ~ ref('int__courseware__extraction_allowlist') ~ ")" %}
    {# the allowlist has no OCW rows yet, so a deployed build reads no OCW course #}
    {% set ocw_course_filter = "('')" %}
{% endif %}
{% set ocw_context_sections = ['syllabus'] %}
{# about the course rather than its content; calendar rows still title and place sessions #}
{% set ocw_skipped_sections = ['calendar', 'related-resources', 'instructor-insights', 'download', 'resource-index'] %}
{% set ocw_textbook_section = "'^((open|online)-)?textbooks?(-|$)'" %}
{% set ocw_transcript_extensions = ['.srt', '.vtt', '.webvtt'] %}
{% set ocw_resource_href = "'href=\"/(courses/[^/\"]+/resources/[^/\"#?]+)'" %}
{# a table column of answer keys: "Solutions", "Exam Solutions", "Answer Key"; not "Problem Sets and Solutions" #}
{% set ocw_solution_column = "'^((problem sets?|psets?|exams?|quiz(zes)?|labs?|homeworks?|assignments?|practice exams?|exam review|sample|recitations?|midterms?|finals?|final exam) )?(solutions?|answers?|answer keys?|keys?|solns?)$'" %}
{% set ocw_table_cell = "'(?s)<t[dh](?:\\s[^>]*)?>(.*?)</t[dh]>'" %}
{% set ocw_problem_set_release = "'(?:^|[^a-z])(?:problem set|pset|ps) ?#? ?([0-9]+),? (?:out|assigned|released|handed out)'" %}

with content as (
    select *
    from {{ ref('dim_course_content') }}
    where
        is_latest
        and platform in ('mitxonline', 'mitxpro', 'residential')
        and courserun_readable_id in {{ courserun_filter }}
)

, xml_blocks as (
    select
        courserun_readable_id
        , case coursestructure_xml_source_system
            when 'xpro' then 'mitxpro'
            when 'mitx' then 'residential'
            else coursestructure_xml_source_system
        end as platform
        , coursestructure_xml_block_type as block_type
        -- the export-relative path, as the exclusions and document text spell it
        , {{ regexp_replace_all('coursestructure_xml_block_path', "'^course/'", "''") }} as export_path
        , coursestructure_xml_raw_xml as raw_xml
        , video_edx_id
        , coursestructure_xml_retrieved_at as retrieved_at
    from {{ ref('int__combined__course_xml_blocks') }}
    where
        coursestructure_xml_source_system in ('mitxonline', 'xpro', 'mitx')
        and courserun_readable_id in {{ courserun_filter }}
)

, latest_retrievals as (
    select
        platform
        , courserun_readable_id
        , max(retrieved_at) as retrieved_at
    from xml_blocks
    group by platform, courserun_readable_id
)

, latest_xml_blocks as (
    select xml_blocks.*
    from xml_blocks
    inner join latest_retrievals as latest
        on
            xml_blocks.platform = latest.platform
            and xml_blocks.courserun_readable_id = latest.courserun_readable_id
            and xml_blocks.retrieved_at = latest.retrieved_at
)

, excluded_paths as (
    select
        case content_file_source_system
            when 'xpro' then 'mitxpro'
            when 'mitx' then 'residential'
            else content_file_source_system
        end as platform
        , courserun_readable_id
        , content_file_path
    from {{ ref('stg__openedx__s3__course_file_exclusions') }}
    where
        content_file_is_excluded
        and content_file_exclusion_reason in ('{{ honored_exclusions | join("', '") }}')
        and courserun_readable_id in {{ courserun_filter }}
)

, documents as (
    select
        case documents.content_file_source_system
            when 'xpro' then 'mitxpro'
            when 'mitx' then 'residential'
            else documents.content_file_source_system
        end as platform
        , documents.courserun_readable_id
        , documents.content_file_path
        , documents.content_file_extension
        , documents.content_file_text
    from {{ ref('stg__openedx__s3__course_document_text') }} as documents
    where
        documents.content_file_extraction_status = 'extracted'
        and documents.courserun_readable_id in {{ courserun_filter }}
)

, transcripts as (
    select
        case content_file_source_system
            when 'xpro' then 'mitxpro'
            when 'mitx' then 'residential'
            else content_file_source_system
        end as platform
        , courserun_readable_id
        , content_file_path
        , content_file_text
    from {{ ref('stg__openedx__s3__course_transcript_text') }}
    where
        content_file_extraction_status = 'extracted'
        and courserun_readable_id in {{ courserun_filter }}
)

-- as in #4095, a chapter's size counts only the sequentials a learner can navigate to,
-- and the others are neither units nor context
, sequentials as (
    select
        sequential.platform
        , sequential.courserun_readable_id
        , sequential.block_id
        , sequential.block_title
        , sequential.block_index
        , lower(trim({{ regexp_replace_all('sequential.block_title', "'\\s+'", "' '") }})) as normalized_title
        , count(*) over (
            partition by sequential.platform, sequential.courserun_readable_id, sequential.chapter_block_id
        ) as chapter_sequential_count
    from content as sequential
    left join content as chapter
        on
            sequential.platform = chapter.platform
            and sequential.courserun_readable_id = chapter.courserun_readable_id
            and sequential.chapter_block_id = chapter.block_id
    where
        sequential.block_category = 'sequential'
        and coalesce({{ json_query_string('sequential.block_metadata', "'$.visible_to_staff_only'") }}, '') != 'true'
        and coalesce({{ json_query_string('chapter.block_metadata', "'$.visible_to_staff_only'") }}, '') != 'true'
        and coalesce({{ json_query_string('sequential.block_metadata', "'$.hide_from_toc'") }}, '') != 'true'
)

, classified_sequentials as (
    select
        *
        , coalesce(
            normalized_title in ('{{ context_sequential_titles | join("', '") }}')
            or (
                chapter_sequential_count = 1
                and {{ regexp_like('normalized_title', "'\\b(intro|overview|welcome|conclusion|summary|wrap)'") }}
            )
            , false
        ) as is_context_sequential
    from sequentials
)

, units as (
    select
        platform
        , courserun_readable_id
        , block_id as scope_id
        , block_title as scope_title
        , row_number() over (
            partition by platform, courserun_readable_id order by block_index
        ) as scope_position
    from classified_sequentials
    where not is_context_sequential
)

, context_sequentials as (
    select
        platform
        , courserun_readable_id
        , block_id as sequential_block_id
    from classified_sequentials
    where is_context_sequential
)

-- a library_content block's children are variants; keep the first
, leaf_blocks as (
    select
        content.platform
        , content.courserun_readable_id
        , content.sequential_block_id
        , content.block_id
        , content.block_category
        , content.block_title
        , content.block_index
        , {{ regexp_extract_or_null('content.block_id', "'block@(.+)$'", 1) }} as block_name
        , row_number() over (
            partition by content.platform, content.courserun_readable_id, content.parent_block_id
            order by content.block_index
        ) as sibling_position
        , parent.block_category as parent_category
    from content
    left join content as parent
        on
            content.platform = parent.platform
            and content.courserun_readable_id = parent.courserun_readable_id
            and content.parent_block_id = parent.block_id
    where content.block_category in ('html', 'problem', 'video')
)

, unit_blocks as (
    select
        leaf_blocks.*
        , latest_xml_blocks.raw_xml
        , latest_xml_blocks.video_edx_id
        -- an html block's body is its own file, named by the block's filename attribute
        , coalesce(
            {{ regexp_extract_or_null('latest_xml_blocks.raw_xml', "'filename=\"([^\"]+)\"'", 1) }}
            , leaf_blocks.block_name
        ) as html_filename
        , {{ regexp_extract_or_null(
            'latest_xml_blocks.raw_xml', "'&quot;en&quot;: &quot;([^&]+)&quot;'", 1
        ) }} as english_transcript_filename
    from leaf_blocks
    -- problems inside library_content are named by their file, not their url_name, so
    -- the join is on the export path for every block type
    inner join latest_xml_blocks
        on
            leaf_blocks.platform = latest_xml_blocks.platform
            and leaf_blocks.courserun_readable_id = latest_xml_blocks.courserun_readable_id
            and latest_xml_blocks.export_path
            = concat(leaf_blocks.block_category, '/', leaf_blocks.block_name, '.xml')
    where leaf_blocks.parent_category != 'library_content' or leaf_blocks.sibling_position = 1
)

, unit_sources as (
    select
        unit_blocks.platform
        , unit_blocks.courserun_readable_id
        , unit_blocks.sequential_block_id as scope_id
        , 1 as source_group
        , unit_blocks.block_index as source_order
        , unit_blocks.block_id as source_ref
        , case unit_blocks.block_category
            when 'video' then 'video_transcript'
            else unit_blocks.block_category
        end as source_kind
        , unit_blocks.block_title as source_title
        , case unit_blocks.block_category
            when 'html' then html_body.content_file_text
            when 'video' then transcript.content_file_text
            when 'problem' then {{ courseware_problem_question_text('unit_blocks.raw_xml') }}
        end as source_text
    from unit_blocks
    left join documents as html_body
        on
            unit_blocks.block_category = 'html'
            and unit_blocks.platform = html_body.platform
            and unit_blocks.courserun_readable_id = html_body.courserun_readable_id
            and html_body.content_file_path = concat('html/', unit_blocks.html_filename, '.html')
    left join transcripts as transcript
        on
            unit_blocks.block_category = 'video'
            and unit_blocks.platform = transcript.platform
            and unit_blocks.courserun_readable_id = transcript.courserun_readable_id
            and transcript.content_file_path = concat('static/', coalesce(
                unit_blocks.english_transcript_filename, concat(unit_blocks.video_edx_id, '-en.srt')
            ))
)

-- answer keys and solution sets are never read (courseware_answer_key); an Open edX file has no title
, unscoped_documents as (
    select documents.*
    from documents
    left join excluded_paths
        on
            documents.platform = excluded_paths.platform
            and documents.courserun_readable_id = excluded_paths.courserun_readable_id
            and documents.content_file_path = excluded_paths.content_file_path
    where
        excluded_paths.content_file_path is null
        and not {{ courseware_answer_key(
            regexp_replace_all('documents.content_file_path', "'[.][A-Za-z0-9]+$'", "''")
            , "''"
            , courseware_text_head('documents.content_file_text')
            , courseware_answer_markers('documents.content_file_text')
        ) }}
)

-- every html body's links, including those of a body Tika failed on or found empty
, html_bodies as (
    select
        case content_file_source_system
            when 'xpro' then 'mitxpro'
            when 'mitx' then 'residential'
            else content_file_source_system
        end as platform
        , courserun_readable_id
        , content_file_path
        , content_file_static_references
    from {{ ref('stg__openedx__s3__course_document_text') }}
    where
        content_file_static_references is not null
        {% if courserun_filter %}and courserun_readable_id in {{ courserun_filter }}{% endif %}
)

, html_links as (
    select
        unit_blocks.platform
        , unit_blocks.courserun_readable_id
        , unit_blocks.sequential_block_id
        , unit_blocks.block_id
        , unit_blocks.block_index
        , {{ json_extract_scalar('linked.static_path', "'$'") }} as linked_path
    from unit_blocks
    inner join html_bodies as html_body
        on
            unit_blocks.block_category = 'html'
            and unit_blocks.platform = html_body.platform
            and unit_blocks.courserun_readable_id = html_body.courserun_readable_id
            and html_body.content_file_path = concat('html/', unit_blocks.html_filename, '.html')
    -- a link places a document only from a sequential that is read, as a unit or as
    -- context; one linked only from a staff-only or hidden sequential stays in materials
    inner join classified_sequentials as sequential
        on
            unit_blocks.platform = sequential.platform
            and unit_blocks.courserun_readable_id = sequential.courserun_readable_id
            and unit_blocks.sequential_block_id = sequential.block_id
    cross join {{ unnest_json_array('html_body.content_file_static_references', 'linked', 'static_path') }}
)

-- Studio stores "my file.pdf" under the asset name my_file.pdf, which is what a link
-- spells, so a link is matched to a document by its name without punctuation. That
-- key also merges distinct files (LectureSlides_15.pdf, Lecture_Slides_15.pdf), so an
-- exact path wins, and the key is used only when it names one document.
, link_matches as (
    select
        html_links.*
        , documents.content_file_path
        , documents.content_file_text
        , case when html_links.linked_path = documents.content_file_path then 1 else 0 end as is_exact
    from html_links
    inner join unscoped_documents as documents
        on
            html_links.platform = documents.platform
            and html_links.courserun_readable_id = documents.courserun_readable_id
            and {{ regexp_replace_all("lower(html_links.linked_path)", "'[^a-z0-9./-]'", "''") }}
            = {{ regexp_replace_all("lower(documents.content_file_path)", "'[^a-z0-9./-]'", "''") }}
    where documents.content_file_extension in ('{{ document_extensions | join("', '") }}')
)

, counted_link_matches as (
    select
        *
        , sum(is_exact) over (
            partition by platform, courserun_readable_id, block_id, linked_path
        ) as exact_matches
        , count(*) over (
            partition by platform, courserun_readable_id, block_id, linked_path
        ) as key_matches
    from link_matches
)

-- A linked document is read right after the html block that links it: it takes the
-- block's source_order, and its static/ path sorts after the block's block-v1 key. One
-- linked from several units is read in the first, so its claims are not extracted twice.
, linked_documents as (
    select
        platform
        , courserun_readable_id
        , sequential_block_id as scope_id
        , block_index as source_order
        , content_file_path as source_ref
        , 'document' as source_kind
        , {{ regexp_replace_all('content_file_path', "'^static/'", "''") }} as source_title
        , content_file_text as source_text
        , row_number() over (
            partition by platform, courserun_readable_id, content_file_path
            order by block_index
        ) as link_rank
    from counted_link_matches
    where is_exact = 1 or (exact_matches = 0 and key_matches = 1)
)

, context_pages as (
    select
        *
        , {{ regexp_extract_or_null('content_file_path', "'^([a-z]+)/'", 1) }} as page_folder
        -- about/short_description.html -> Short description
        , {{ regexp_replace_all(
            regexp_replace_all(regexp_replace_all('content_file_path', "'^about/'", "''"), "'\\.html$'", "''")
            , "'_'", "' '"
        ) }} as about_field
    from unscoped_documents
    where content_file_extension = '.html'
)

, context_sources as (
    select
        platform
        , courserun_readable_id
        , 'context' as scope_id
        , 0 as source_group
        , row_number() over (
            partition by platform, courserun_readable_id order by content_file_path
        ) as source_order
        , content_file_path as source_ref
        , concat(page_folder, '_page') as source_kind
        , content_file_path as source_title
        , case
            when page_folder = 'about' and content_file_path not in ('{{ description_pages | join("', '") }}')
                then concat(
                    upper(substr(about_field, 1, 1)), substr(about_field, 2), {{ codepoint_char(10) }}, content_file_text
                )
            else content_file_text
        end as source_text
    from context_pages
    where page_folder in ('about', 'tabs', 'info')

    union all

    select
        unit_sources.platform
        , unit_sources.courserun_readable_id
        , 'context' as scope_id
        , unit_sources.source_group
        , unit_sources.source_order
        , unit_sources.source_ref
        , unit_sources.source_kind
        , unit_sources.source_title
        , unit_sources.source_text
    from unit_sources
    inner join context_sequentials
        on
            unit_sources.platform = context_sequentials.platform
            and unit_sources.courserun_readable_id = context_sequentials.courserun_readable_id
            and unit_sources.scope_id = context_sequentials.sequential_block_id

    union all

    select
        linked_documents.platform
        , linked_documents.courserun_readable_id
        , 'context' as scope_id
        , 1 as source_group
        , linked_documents.source_order
        , linked_documents.source_ref
        , linked_documents.source_kind
        , linked_documents.source_title
        , linked_documents.source_text
    from linked_documents
    inner join context_sequentials
        on
            linked_documents.platform = context_sequentials.platform
            and linked_documents.courserun_readable_id = context_sequentials.courserun_readable_id
            and linked_documents.scope_id = context_sequentials.sequential_block_id
    where linked_documents.link_rank = 1
)

, material_sources as (
    select
        unscoped_documents.platform
        , unscoped_documents.courserun_readable_id
        , 'materials' as scope_id
        , 0 as source_group
        , row_number() over (
            partition by unscoped_documents.platform, unscoped_documents.courserun_readable_id
            order by unscoped_documents.content_file_path
        ) as source_order
        , unscoped_documents.content_file_path as source_ref
        , 'document' as source_kind
        , {{ regexp_replace_all('unscoped_documents.content_file_path', "'^static/'", "''") }} as source_title
        , unscoped_documents.content_file_text as source_text
    from unscoped_documents
    left join linked_documents
        on
            unscoped_documents.platform = linked_documents.platform
            and unscoped_documents.courserun_readable_id = linked_documents.courserun_readable_id
            and unscoped_documents.content_file_path = linked_documents.source_ref
            and linked_documents.link_rank = 1
    where
        unscoped_documents.content_file_path like 'static/%'
        and unscoped_documents.content_file_extension in ('{{ document_extensions | join("', '") }}')
        and linked_documents.source_ref is null
)

, ocw_courses as (
    select
        course_slug
        , courserun_readable_id
        , course_title
        , course_description_html
    from {{ ref('int__ocw__live_courses') }}
    where
        courserun_readable_id is not null
        and course_slug in {{ ocw_course_filter }}
)

, ocw_named_files as (
    select
        files.courserun_readable_id
        , files.course_slug
        , files.content_file_key
        , files.content_type
        , files.title
        , files.file_extension
        , files.content
        , coalesce(files.content_tags, '') as content_tags
        , lower(coalesce(files.title, '')) as title_lower
        , {{ courseware_ocw_resource_name('files.content_file_key') }} as file_name
        , case
            when files.content_type = 'page'
                then {{ courseware_text_head(regexp_replace_all('substr(files.content, 1, 3000)', "'<[^>]*>'", "' '")) }}
            else {{ courseware_text_head('files.content') }}
        end as text_head
        , {{ courseware_answer_markers('files.content') }} as answer_markers
    from {{ ref('int__ocw__content_files') }} as files
    inner join ocw_courses
        on files.course_slug = ocw_courses.course_slug
)

, ocw_files as (
    select
        courserun_readable_id
        , course_slug
        , content_file_key
        , content_type
        , title
        , file_extension
        -- a page's key is courses/<slug>/pages/<path>/; the course's root page has no path
        , {{ regexp_extract_or_null('content_file_key', "'/pages/(.+)$'", 1) }} as page_path
        , {{ regexp_extract_or_null('content_file_key', "'/pages/([^/]+)/'", 1) }} as section
        , {{ regexp_replace_all('content_file_key', "'[^/]+/$'", "''") }} as parent_key
        , (
            -- a section page's opening lists its problem sets and their solutions, so a
            -- page is judged without its opening
            {{ courseware_ocw_answer_resource(
                'file_name', 'title_lower', 'content_type', 'file_extension', 'content_tags'
                , "case when content_type != 'page' then text_head end", 'answer_markers'
            ) }}
            -- a page of "Answer:" entries on homework, e.g. virtual office hours
            or (
                content_type = 'page'
                and content_tags not like '%Videos%'
                and coalesce(answer_markers, 0) >= 3
                and {{ regexp_like('text_head', "'problem sets?|psets?|homework|exams?|quiz|midterm'") }}
            )
        )
        -- a page of problems and their solutions keeps its problems; its solution
        -- resources are left out on their own
        and not (
            content_type = 'page'
            and {{ regexp_like("concat(title_lower, ' ', file_name)", "'(and|&|with|/)[ _-]solutions?|solutions?[ _-](and|&)'") }}
        ) as is_solution
        , case
            when content_type = 'page' then {{ courseware_ocw_answers_removed('content') }}
            else content
        end as body
    from ocw_named_files
)

, ocw_page_children as (
    select
        parent_key as page_key
        , count(*) as child_count
    from ocw_files
    where page_path is not null
    group by parent_key
)

-- counted by section: RES.8-005's one Problem Solving Videos section has 11 child
-- pages with children of their own
, ocw_taught_by_groups as (
    select files.courserun_readable_id
    from ocw_files as files
    inner join ocw_page_children as children
        on files.content_file_key = children.page_key
    where
        files.page_path is not null
        and files.section not in ('{{ (ocw_context_sections + ocw_skipped_sections) | join("', '") }}')
        and not {{ regexp_like('files.section', ocw_textbook_section) }}
    group by files.courserun_readable_id
    having count(distinct files.section) >= 3
)

, ocw_pages as (
    select
        files.courserun_readable_id
        , files.course_slug
        , files.content_file_key as page_key
        , files.title
        , files.section
        , files.parent_key
        , files.body
        , length(files.page_path) - length(replace(files.page_path, '/', '')) as depth
        , coalesce(children.child_count, 0) > 0 as has_children
        , files.is_solution
        -- a solution page with children keeps them: "In-Class Questions and Video
        -- Solutions" holds the questions, and its own text and links are not read
        , case
            when files.is_solution and coalesce(children.child_count, 0) = 0 then 'solution'
            when files.section in ('{{ ocw_context_sections | join("', '") }}') then 'context'
            when files.section in ('{{ ocw_skipped_sections | join("', '") }}') then 'skipped'
            when
                grouped.courserun_readable_id is not null
                and {{ regexp_like('files.section', ocw_textbook_section) }}
                then 'reference'
            else 'unit'
        end as page_role
    from ocw_files as files
    left join ocw_page_children as children
        on files.content_file_key = children.page_key
    left join ocw_taught_by_groups as grouped
        on files.courserun_readable_id = grouped.courserun_readable_id
    where files.page_path is not null
)

-- A page with children is a unit, and so is a top-level section page; any other page
-- belongs to its parent's unit. A page whose parent the site does not publish is a unit
-- of its own (8.02T's visualizations/ pages have no visualizations page).
, ocw_unit_pages as (
    select
        pages.*
        , case
            when pages.has_children or pages.depth = 1 or parents.page_key is null then pages.page_key
            else pages.parent_key
        end as unit_key
        , pages.depth = 1 and not pages.has_children as is_flat_section
    from ocw_pages as pages
    left join ocw_pages as parents
        on pages.parent_key = parents.page_key
    where pages.page_role = 'unit'
)

, ocw_page_hrefs as (
    select
        courserun_readable_id
        , page_key
        , case when is_solution then 'solution' else page_role end as page_role
        , {{ regexp_extract_all('body', ocw_resource_href, 1) }} as hrefs
    from ocw_pages
    where {{ array_length(regexp_extract_all('body', ocw_resource_href, 1)) }} > 0
)

, ocw_direct_page_links as (
    select
        page_hrefs.courserun_readable_id
        , page_hrefs.page_key
        , page_hrefs.page_role
        , links.link_index
        , concat({{ element_at_array('page_hrefs.hrefs', 'links.link_index') }}, '/') as target_key
    from ocw_page_hrefs as page_hrefs
    cross join {{ unnest_sequence(array_length('page_hrefs.hrefs'), 'links', 'link_index') }}
)

-- the resources a resource's description links, which int__ocw__content_files cannot
-- give for a video it dropped
, ocw_description_hrefs as (
    select
        ocw_courses.courserun_readable_id
        , substr(ocw_content.coursecontent_s3_key, 1, length(ocw_content.coursecontent_s3_key) - length('data.json'))
            as resource_key
        , {{ regexp_extract_all('ocw_content.coursecontent_body', ocw_resource_href, 1) }} as hrefs
    from {{ ref('stg__ocw__s3__course_content') }} as ocw_content
    inner join ocw_courses
        on ocw_content.course_slug = ocw_courses.course_slug
    where
        ocw_content.coursecontent_kind = 'resource'
        and {{ array_length(regexp_extract_all('ocw_content.coursecontent_body', ocw_resource_href, 1)) }} > 0
)

, ocw_description_links as (
    select
        description_hrefs.courserun_readable_id
        , description_hrefs.resource_key
        , links.link_index
        , concat({{ element_at_array('description_hrefs.hrefs', 'links.link_index') }}, '/') as target_key
    from ocw_description_hrefs as description_hrefs
    cross join {{ unnest_sequence(array_length('description_hrefs.hrefs'), 'links', 'link_index') }}
)

-- a page links what it links directly and, right after each resource, what that
-- resource's description links
, ocw_page_links as (
    select
        courserun_readable_id
        , page_key
        , page_role
        , cast(link_index as bigint) * 1000 as link_index
        , target_key
    from ocw_direct_page_links

    union all

    select
        page_links.courserun_readable_id
        , page_links.page_key
        , page_links.page_role
        , cast(page_links.link_index as bigint) * 1000 + description_links.link_index as link_index
        , description_links.target_key
    from ocw_direct_page_links as page_links
    inner join ocw_description_links as description_links
        on
            page_links.courserun_readable_id = description_links.courserun_readable_id
            and page_links.target_key = description_links.resource_key
)

-- Every page's tables, one row per table row. On top-level section pages and the
-- calendar, a row whose first cell is a number, in a table whose first header names
-- sessions, lectures or weeks, is that session's row.
, ocw_row_pages as (
    select
        pages.courserun_readable_id
        , pages.page_key
        , pages.section
        , coalesce(unit_pages.is_flat_section, false) or pages.section = 'calendar' as reads_sessions
        , {{ regexp_split('pages.body', "'<table'") }} as tables
    from ocw_pages as pages
    left join ocw_unit_pages as unit_pages
        on pages.page_key = unit_pages.page_key
    where pages.body like '%<table%'
)

, ocw_tables as (
    select
        row_pages.courserun_readable_id
        , row_pages.page_key
        , row_pages.section
        , row_pages.reads_sessions
        , page_tables.table_index
        , {{ regexp_split(
            "split_part(" ~ element_at_array('row_pages.tables', 'page_tables.table_index') ~ ", '</table>', 1)"
            , "'<tr'"
        ) }} as table_rows
    from ocw_row_pages as row_pages
    cross join {{ unnest_sequence(array_length('row_pages.tables'), 'page_tables', 'table_index') }}
    where page_tables.table_index > 1
)

, ocw_table_rows as (
    select
        tables.courserun_readable_id
        , tables.page_key
        , tables.section
        , tables.reads_sessions
        , tables.table_index
        , table_rows.row_index
        , {{ element_at_array('tables.table_rows', 'table_rows.row_index') }} as row_html
        -- the table's first cell, its header
        , {{ regexp_extract_or_null(element_at_array('tables.table_rows', 2), ocw_table_cell, 1) }} as header_html
    from ocw_tables as tables
    cross join {{ unnest_sequence(array_length('tables.table_rows'), 'table_rows', 'row_index') }}
    where table_rows.row_index > 1
)

-- the header cells of each table's first row, and the columns among them that hold
-- answer keys
, ocw_table_headers as (
    select
        courserun_readable_id
        , page_key
        , table_index
        , {{ regexp_extract_all(element_at_array('table_rows', 2), ocw_table_cell, 1) }} as header_cells
    from ocw_tables
    where {{ array_length('table_rows') }} > 1
)

, ocw_table_columns as (
    select
        headers.courserun_readable_id
        , headers.page_key
        , headers.table_index
        , {{ array_length('headers.header_cells') }} as column_count
        , columns.column_index
        , lower({{ courseware_html_text(element_at_array('headers.header_cells', 'columns.column_index')) }})
            as header_text
    from ocw_table_headers as headers
    cross join {{ unnest_sequence(array_length('headers.header_cells'), 'columns', 'column_index') }}
    where {{ array_length('headers.header_cells') }} > 0
)

-- a row whose cells line up with the header's; rows spanning columns are left out
, ocw_solution_cells as (
    select
        table_rows.courserun_readable_id
        , {{ regexp_extract_all(
            element_at_array(regexp_extract_all('table_rows.row_html', ocw_table_cell, 1), 'columns.column_index')
            , ocw_resource_href
            , 1
        ) }} as hrefs
    from ocw_table_rows as table_rows
    inner join ocw_table_columns as columns
        on
            table_rows.page_key = columns.page_key
            and table_rows.table_index = columns.table_index
    where
        table_rows.row_index > 2
        and {{ regexp_like("replace(columns.header_text, '&amp;', '&')", ocw_solution_column) }}
        and {{ array_length(regexp_extract_all('table_rows.row_html', ocw_table_cell, 1)) }} = columns.column_count
)

, ocw_solution_resources as (
    select distinct
        solution_cells.courserun_readable_id
        , concat({{ element_at_array('solution_cells.hrefs', 'links.link_index') }}, '/') as content_file_key
    from ocw_solution_cells as solution_cells
    cross join {{ unnest_sequence(array_length('solution_cells.hrefs'), 'links', 'link_index') }}
    where {{ array_length('solution_cells.hrefs') }} > 0

    union

    select
        courserun_readable_id
        , content_file_key
    from ocw_files
    where content_type != 'page' and is_solution
)

-- OCW publishes one file under several resources; a copy of a key is a key
, ocw_solution_texts as (
    select distinct
        files.courserun_readable_id
        , {{ dbt.hash('files.body') }} as text_hash
    from ocw_files as files
    inner join ocw_solution_resources as solution_resources
        on
            files.courserun_readable_id = solution_resources.courserun_readable_id
            and files.content_file_key = solution_resources.content_file_key
    -- a scan with no text layer reads as its file name, which a problem set can share
    where length(files.body) > 200
)

, ocw_resources as (
    select
        files.courserun_readable_id
        , files.content_file_key
        , files.content_type
        , files.file_extension
        , files.title
        , files.body as resource_text
        , case
            when
                files.content_type = 'video'
                or files.file_extension in ('{{ ocw_transcript_extensions | join("', '") }}')
                or lower(files.title) like '%transcript'
                then 'video_transcript'
            else 'document'
        end as source_kind
    from ocw_files as files
    left join ocw_solution_resources as solution_resources
        on
            files.courserun_readable_id = solution_resources.courserun_readable_id
            and files.content_file_key = solution_resources.content_file_key
    left join ocw_solution_texts as solution_texts
        on
            files.courserun_readable_id = solution_texts.courserun_readable_id
            and {{ dbt.hash('files.body') }} = solution_texts.text_hash
    where
        files.content_type != 'page'
        and files.body is not null
        and solution_resources.content_file_key is null
        and solution_texts.text_hash is null
)

, ocw_table_cells as (
    select
        courserun_readable_id
        , page_key
        , section
        , table_index
        , row_index
        , row_html
        , lower({{ courseware_html_text('header_html') }}) as header_text
        , {{ courseware_html_text(element_at_array(regexp_extract_all('row_html', ocw_table_cell, 1), 1)) }}
            as first_cell_text
        , {{ courseware_html_text(element_at_array(regexp_extract_all('row_html', ocw_table_cell, 1), 2)) }}
            as second_cell_text
    from ocw_table_rows
    where reads_sessions
)

, ocw_session_rows as (
    select
        courserun_readable_id
        , page_key
        , section
        , table_index
        , row_index
        , case
            when {{ regexp_like('header_text', "'^(week|wk)'") }} then 'week'
            else 'session'
        end as session_kind
        , case
            when {{ regexp_like('header_text', "'^(week|wk)'") }} then 'Week'
            when {{ regexp_like('header_text', "'^lec'") }} then 'Lecture'
            when {{ regexp_like('header_text', "'^class'") }} then 'Class'
            when {{ regexp_like('header_text', "'^day'") }} then 'Day'
            else 'Session'
        end as session_label
        , {{ try_cast('first_cell_text', 'integer') }} as session_number
        -- link labels such as "(PDF - 1.2MB)" are not the topic
        , {{ strip_whitespace(regexp_replace_all(
            'second_cell_text', "'\\((PDF|ZIP|PY|TXT|MP4|MP3|PPT|PPTX|DOC|DOCX|XLS|XLSX|IPYNB)\\b[^)]*\\)'", "''"
        )) }} as session_topic
        , row_html
    from ocw_table_cells
    where
        {{ regexp_like('header_text', "'^(ses|session|lec|lecture|class|day|l|week|wk)s?\\b'") }}
        and {{ regexp_like('first_cell_text', "'^[0-9]+$'") }}
)

, ocw_session_hrefs as (
    select
        courserun_readable_id
        , page_key
        , concat(session_kind, '-', cast(session_number as varchar)) as scope_id
        , table_index
        , row_index
        , {{ regexp_extract_all('row_html', ocw_resource_href, 1) }} as hrefs
    from ocw_session_rows
    where {{ array_length(regexp_extract_all('row_html', ocw_resource_href, 1)) }} > 0
)

, ocw_direct_session_links as (
    select
        session_hrefs.courserun_readable_id
        , session_hrefs.page_key
        , session_hrefs.scope_id
        , (cast(session_hrefs.table_index as bigint) * 1000000 + session_hrefs.row_index * 1000 + links.link_index)
        * 1000 as link_order
        , concat({{ element_at_array('session_hrefs.hrefs', 'links.link_index') }}, '/') as target_key
    from ocw_session_hrefs as session_hrefs
    cross join {{ unnest_sequence(array_length('session_hrefs.hrefs'), 'links', 'link_index') }}
)

, ocw_release_rows as (
    select
        courserun_readable_id
        , page_key
        , concat(session_kind, '-', cast(session_number as varchar)) as scope_id
        , table_index
        , row_index
        , {{ regexp_extract_all("lower(" ~ courseware_html_text('row_html') ~ ")", ocw_problem_set_release, 1) }}
            as problem_sets
    from ocw_session_rows
    where section = 'calendar'
)

-- the session whose calendar row first says problem set N is out
, ocw_problem_set_releases as (
    select
        release_rows.courserun_readable_id
        , release_rows.page_key
        , release_rows.scope_id
        , (cast(release_rows.table_index as bigint) * 1000000 + release_rows.row_index * 1000 + 999) * 1000
        + problem_sets.problem_set_index as link_order
        , cast(cast({{ element_at_array('release_rows.problem_sets', 'problem_sets.problem_set_index') }} as integer)
            as varchar) as problem_set
        , row_number() over (
            partition by
                release_rows.courserun_readable_id
                , cast({{ element_at_array('release_rows.problem_sets', 'problem_sets.problem_set_index') }} as integer)
            order by release_rows.table_index, release_rows.row_index
        ) as release_rank
    from ocw_release_rows as release_rows
    cross join {{ unnest_sequence(array_length('release_rows.problem_sets'), 'problem_sets', 'problem_set_index') }}
    where {{ array_length('release_rows.problem_sets') }} > 0
)

-- a resource titled or named as problem set N ("Problem Set 3", ps03, mit6_046_pset3)
-- that no session row links goes to the session that releases it
, ocw_release_links as (
    select
        releases.courserun_readable_id
        , releases.page_key
        , releases.scope_id
        , releases.link_order
        , resources.content_file_key as target_key
    from ocw_problem_set_releases as releases
    inner join ocw_resources as resources
        on
            releases.courserun_readable_id = resources.courserun_readable_id
            and (
                {{ regexp_like(
                    'lower(resources.title)'
                    , "concat('(^|[^a-z])(problem set|pset|ps) ?#? ?0*', releases.problem_set, '([^0-9]|$)')"
                ) }}
                or {{ regexp_like(
                    regexp_extract_or_null('resources.content_file_key', "'([^/]+)/$'", 1)
                    , "concat('(^|[^a-z])(ps|pset|problemset)[_-]?0*', releases.problem_set, '([^0-9]|$)')"
                ) }}
            )
    left join ocw_direct_session_links as session_links
        on
            resources.courserun_readable_id = session_links.courserun_readable_id
            and resources.content_file_key = session_links.target_key
    where
        releases.release_rank = 1
        and resources.source_kind = 'document'
        and session_links.target_key is null
)

, ocw_session_links as (
    select * from ocw_direct_session_links

    union all

    select * from ocw_release_links

    union all

    select
        session_links.courserun_readable_id
        , session_links.page_key
        , session_links.scope_id
        , session_links.link_order + description_links.link_index as link_order
        , description_links.target_key
    from ocw_direct_session_links as session_links
    inner join ocw_description_links as description_links
        on
            session_links.courserun_readable_id = description_links.courserun_readable_id
            and session_links.target_key = description_links.resource_key
)

-- a session is titled from the calendar where it has the row
, ocw_session_titles as (
    select
        courserun_readable_id
        , session_kind
        , session_number
        , session_label
        , session_topic
        , row_number() over (
            partition by courserun_readable_id, session_kind, session_number
            order by case when section = 'calendar' then 0 else 1 end, page_key, table_index, row_index
        ) as title_rank
    from ocw_session_rows
)

, ocw_session_units as (
    select
        courserun_readable_id
        , concat(session_kind, '-', cast(session_number as varchar)) as scope_id
        , case
            when coalesce(session_topic, '') = '' then concat(session_label, ' ', cast(session_number as varchar))
            -- the topic cell often already reads "Lecture 3: ..."
            when {{ regexp_like(
                'lower(session_topic)'
                , "concat('^(lecture|lec|session|ses|class|week|day|l) ?#? ?', cast(session_number as varchar), '([^0-9]|$)')"
            ) }}
                then session_topic
            else concat(session_label, ' ', cast(session_number as varchar), ': ', session_topic)
        end as scope_title
        , 0 as group_order
        , session_number + case session_kind when 'week' then 100000 else 0 end as sort_number
        , '' as sort_key
        , 0 as sort_offset
    from ocw_session_titles
    where title_rank = 1
)

, ocw_page_units as (
    select
        units.courserun_readable_id
        , units.page_key as scope_id
        , case
            when units.depth = 1 or parent.title is null then units.title
            else concat(parent.title, ' / ', units.title)
        end as scope_title
        , case when units.is_flat_section then 2 else 1 end as group_order
        , case
            when not units.is_flat_section
                then {{ try_cast(regexp_extract_or_null('top.title', "'([0-9]+)'", 1), 'integer') }}
        end as sort_number
        , top.page_key as sort_key
        , case
            when units.depth = 1 then 0
            else nullif({{ string_position("concat('/', units.page_key)", 'top.body') }}, 0)
        end as sort_offset
    from ocw_unit_pages as units
    left join ocw_pages as parent
        on units.parent_key = parent.page_key
    left join ocw_pages as top
        on {{ regexp_extract_or_null('units.page_key', "'^(courses/[^/]+/pages/[^/]+/)'", 1) }} = top.page_key
    where units.unit_key = units.page_key
)

, ocw_unit_scopes as (
    select
        courserun_readable_id
        , scope_id
        , scope_title
        , row_number() over (
            partition by courserun_readable_id
            order by group_order, sort_number nulls first, sort_key, sort_offset nulls last, scope_id
        ) as scope_position
    from (
        select * from ocw_session_units
        union all
        select * from ocw_page_units
    ) as units
)

-- a link a session row took is not read again in its page's unit
, ocw_unit_links as (
    select
        page_links.courserun_readable_id
        , unit_pages.unit_key as scope_id
        , page_links.page_key
        , page_links.link_index
        , page_links.target_key
    from ocw_page_links as page_links
    inner join ocw_unit_pages as unit_pages
        on
            page_links.page_key = unit_pages.page_key
            and page_links.page_role = 'unit'
    left join ocw_session_links as session_links
        on
            page_links.page_key = session_links.page_key
            and page_links.target_key = session_links.target_key
    where session_links.target_key is null
)

-- a unit page's place among its unit's pages: first if it is the unit, else where the
-- unit page links it
, ocw_unit_members as (
    select
        unit_pages.page_key
        , unit_pages.unit_key
        , unit_pages.title
        , unit_pages.body
        , unit_pages.is_flat_section
        , unit_pages.is_solution
        , case
            when unit_pages.page_key = unit_pages.unit_key then 0
            else coalesce(nullif({{ string_position("concat('/', unit_pages.page_key)", 'units.body') }}, 0), 1000000000)
        end as member_order
    from ocw_unit_pages as unit_pages
    inner join ocw_pages as units
        on unit_pages.unit_key = units.page_key
)

, ocw_placed_sources as (
    select
        ocw_courses.courserun_readable_id
        , 'context' as scope_id
        , 0 as member_order
        , '' as member_key
        , 0 as link_order
        , concat('courses/', ocw_courses.course_slug, '/') as source_ref
        , 'course_description' as source_kind
        , 'Course description' as source_title
        , {{ courseware_html_text('ocw_courses.course_description_html') }} as source_text
    from ocw_courses

    union all

    select
        courserun_readable_id
        , 'context' as scope_id
        , depth as member_order
        , page_key as member_key
        , 0 as link_order
        , page_key as source_ref
        , 'page' as source_kind
        , title as source_title
        , {{ courseware_html_text('body') }} as source_text
    from ocw_pages
    where page_role = 'context' and not is_solution

    union all

    select
        page_links.courserun_readable_id
        , 'context' as scope_id
        , pages.depth as member_order
        , pages.page_key as member_key
        , page_links.link_index as link_order
        , resources.content_file_key as source_ref
        , resources.source_kind
        , resources.title as source_title
        , resources.resource_text as source_text
    from ocw_page_links as page_links
    inner join ocw_pages as pages
        on page_links.page_key = pages.page_key
    inner join ocw_resources as resources
        on
            page_links.courserun_readable_id = resources.courserun_readable_id
            and page_links.target_key = resources.content_file_key
    where page_links.page_role = 'context'

    union all

    -- a section page whose resources all went to sessions is only their outline
    select
        members.courserun_readable_id
        , members.unit_key as scope_id
        , members.member_order
        , members.page_key as member_key
        , 0 as link_order
        , members.page_key as source_ref
        , 'page' as source_kind
        , members.title as source_title
        , {{ courseware_html_text('members.body') }} as source_text
    from (
        select
            unit_pages.courserun_readable_id
            , ocw_unit_members.*
        from ocw_unit_members
        inner join ocw_unit_pages as unit_pages
            on ocw_unit_members.page_key = unit_pages.page_key
    ) as members
    where
        not members.is_solution
        and (
            not members.is_flat_section
            or members.page_key not in (select session_links.page_key from ocw_session_links as session_links)
            or members.page_key in (select unit_links.page_key from ocw_unit_links as unit_links)
        )

    union all

    select
        unit_links.courserun_readable_id
        , unit_links.scope_id
        , members.member_order
        , unit_links.page_key as member_key
        , unit_links.link_index as link_order
        , resources.content_file_key as source_ref
        , resources.source_kind
        , resources.title as source_title
        , resources.resource_text as source_text
    from ocw_unit_links as unit_links
    inner join ocw_unit_members as members
        on unit_links.page_key = members.page_key
    inner join ocw_resources as resources
        on
            unit_links.courserun_readable_id = resources.courserun_readable_id
            and unit_links.target_key = resources.content_file_key

    union all

    select
        session_links.courserun_readable_id
        , session_links.scope_id
        , 0 as member_order
        , session_links.page_key as member_key
        , session_links.link_order
        , resources.content_file_key as source_ref
        , resources.source_kind
        , resources.title as source_title
        , resources.resource_text as source_text
    from ocw_session_links as session_links
    inner join ocw_resources as resources
        on
            session_links.courserun_readable_id = resources.courserun_readable_id
            and session_links.target_key = resources.content_file_key

    union all

    -- documents no read page links; what only a skipped page links counts as unlinked,
    -- what a textbook read as reference links as linked
    select
        resources.courserun_readable_id
        , 'materials' as scope_id
        , 0 as member_order
        , '' as member_key
        , 0 as link_order
        , resources.content_file_key as source_ref
        , resources.source_kind
        , resources.title as source_title
        , resources.resource_text as source_text
    from ocw_resources as resources
    left join (
        select distinct
            courserun_readable_id
            , target_key
        from ocw_page_links
        where page_role not in ('skipped', 'solution')
    ) as linked
        on
            resources.courserun_readable_id = linked.courserun_readable_id
            and resources.content_file_key = linked.target_key
    left join (
        select distinct
            courserun_readable_id
            , target_key
        from ocw_session_links
    ) as session_linked
        on
            resources.courserun_readable_id = session_linked.courserun_readable_id
            and resources.content_file_key = session_linked.target_key
    where
        linked.target_key is null
        and session_linked.target_key is null
        and (
            resources.content_type = 'pdf'
            or resources.file_extension in ('{{ document_extensions | join("', '") }}')
        )
)

, ocw_positioned_sources as (
    select
        placed.*
        , case placed.scope_id
            when 'context' then 'context'
            when 'materials' then 'materials'
            else 'unit'
        end as scope_kind
        , case placed.scope_id
            when 'context' then 'Course context'
            when 'materials' then 'Course materials'
            else unit_scopes.scope_title
        end as scope_title
        , case placed.scope_id
            when 'context' then 0
            when 'materials' then 9999
            else unit_scopes.scope_position
        end as scope_position
    from ocw_placed_sources as placed
    left join ocw_unit_scopes as unit_scopes
        on
            placed.courserun_readable_id = unit_scopes.courserun_readable_id
            and placed.scope_id = unit_scopes.scope_id
    where {{ strip_whitespace("coalesce(placed.source_text, '')") }} != ''
)

-- OCW publishes one file under several resources, and a document can be linked from
-- several scopes; each text is read once, in the first unit that has it
, ocw_scoped_sources as (
    select
        'ocw' as platform
        , courserun_readable_id
        , scope_id
        , row_number() over (
            partition by courserun_readable_id, scope_id
            order by member_order, member_key, link_order, source_ref
        ) as source_order
        , source_ref
        , source_kind
        , source_title
        , source_text
        , scope_kind
        , scope_title
        -- numbered again over the units left with text
        , case
            when scope_kind = 'unit'
                then dense_rank() over (partition by courserun_readable_id, scope_kind order by scope_position)
            else scope_position
        end as scope_position
    from (
        select
            *
            , row_number() over (
                partition by courserun_readable_id, {{ dbt.hash('source_text') }}
                order by
                    case scope_kind when 'unit' then 0 when 'context' then 1 else 2 end
                    , scope_position, member_order, member_key, link_order, source_ref
            ) as text_rank
        from ocw_positioned_sources
    ) as ranked
    where text_rank = 1
)

, scoped_sources as (
    select
        unit_sources.*
        , 'unit' as scope_kind
        , units.scope_title
        , units.scope_position
    from unit_sources
    inner join units
        on
            unit_sources.platform = units.platform
            and unit_sources.courserun_readable_id = units.courserun_readable_id
            and unit_sources.scope_id = units.scope_id

    union all

    select
        linked_documents.platform
        , linked_documents.courserun_readable_id
        , linked_documents.scope_id
        , 1 as source_group
        , linked_documents.source_order
        , linked_documents.source_ref
        , linked_documents.source_kind
        , linked_documents.source_title
        , linked_documents.source_text
        , 'unit' as scope_kind
        , units.scope_title
        , units.scope_position
    from linked_documents
    inner join units
        on
            linked_documents.platform = units.platform
            and linked_documents.courserun_readable_id = units.courserun_readable_id
            and linked_documents.scope_id = units.scope_id
    where linked_documents.link_rank = 1

    union all

    select
        context_sources.*
        , 'context' as scope_kind
        , 'Course context' as scope_title
        , 0 as scope_position
    from context_sources

    union all

    select
        material_sources.*
        , 'materials' as scope_kind
        , 'Course materials' as scope_title
        , 9999 as scope_position
    from material_sources

    union all

    select
        platform
        , courserun_readable_id
        , scope_id
        , 0 as source_group
        , source_order
        , source_ref
        , source_kind
        , source_title
        , source_text
        , scope_kind
        , scope_title
        , scope_position
    from ocw_scoped_sources
)

, course_titles as (
    select
        platform
        , courserun_readable_id
        , block_title as course_title
    from content
    where block_category = 'course'

    union all

    select
        'ocw' as platform
        , courserun_readable_id
        , course_title
    from ocw_courses
)

select
    scoped_sources.platform
    , scoped_sources.courserun_readable_id
    , course_titles.course_title
    , scoped_sources.scope_id
    , scoped_sources.scope_kind
    , scoped_sources.scope_title
    , scoped_sources.scope_position
    , row_number() over (
        partition by scoped_sources.platform, scoped_sources.courserun_readable_id, scoped_sources.scope_id
        order by scoped_sources.source_group, scoped_sources.source_order, scoped_sources.source_ref
    ) as source_position
    , scoped_sources.source_ref
    , scoped_sources.source_kind
    , scoped_sources.source_title
    , scoped_sources.source_text
from scoped_sources
left join course_titles
    on
        scoped_sources.platform = course_titles.platform
        and scoped_sources.courserun_readable_id = course_titles.courserun_readable_id
where {{ strip_whitespace("coalesce(scoped_sources.source_text, '')") }} != ''
