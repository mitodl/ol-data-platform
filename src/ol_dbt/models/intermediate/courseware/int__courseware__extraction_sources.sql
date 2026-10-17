{#
    The text an extractor reads for each scope of an Open edX course run, one row per
    source item in reading order.

    Scopes:
      unit       one per sequential, its leaf blocks in course-tree order (dim_course_content)
      context    the about pages, reachable tabs and course info pages
      materials  the run's static documents (slides, PDFs). The html bodies Tika extracts
                 keep no hrefs, so a document cannot be placed in the sequential that links
                 it; they are read together instead.

    Problem text is the question side only (courseware_problem_question_text). A
    library_content block contributes its first problem, since its children are
    randomized variants of one question. Files are dropped by the export's own exclusion
    rules, except about_page: Learn leaves the about pages out of search, but the
    description and prerequisites stated there are what the context scope is for.

    var('courseware_courseruns') limits the build to a list of runs, for local
    development against production text.

    OCW courses (platform ocw, keyed by Learn's run id) are read from the published
    site, int__ocw__content_files:

      unit       a page that has child pages, with its children (a week of an OCW Scholar
                 course, a part of a unit); a top-level section page (Lecture Notes,
                 Assignments, Exams); and, in a flat course, one per session: the
                 resources linked in row N of any table keyed by SES #, LEC # or WEEK #,
                 titled from the calendar
      context    the course description and the syllabus pages, with what they link
      materials  documents no page links

    Answer keys are left out: a resource whose learning resource types name solutions,
    that courseware_answer_key reads as one from its file name, title and opening text,
    that a table links in a column headed Solutions (or Answers, Key), or whose text is
    the same as such a resource's; a page the same way by its name and title, except one
    that holds problems and solutions together ("Problem Sets and Solutions"), whose
    solution resources go instead. What only a solution page links is not read, and not
    counted as linked for materials. A solution page with child pages keeps them as a
    unit, unread itself. Quiz solutions, correctness icons and answer, hint and sample
    answer reveal blocks are cut from page HTML before its links are read. A document
    linked from more than one scope is read once, in the first unit.

    var('courseware_ocw_courses') limits OCW to a list of course slugs. Setting either
    selector alone builds only what it selects.
#}
{% set courseruns = var('courseware_courseruns', []) %}
{% set ocw_courses = var('courseware_ocw_courses', []) %}
{% set honored_exclusions = ['unreferenced_static', 'unreachable_tab', 'legacy_announcements', 'non_content', 'course_settings'] %}
{% set document_extensions = ['.pdf', '.pptx', '.ppt', '.docx', '.doc', '.txt', '.md'] %}

{% set courserun_filter = "('" ~ courseruns | join("', '") ~ "')" if courseruns else ("('')" if ocw_courses else none) %}
{% set ocw_course_filter = "('" ~ ocw_courses | join("', '") ~ "')" if ocw_courses else ("('')" if courseruns else none) %}
{% set ocw_context_sections = ['syllabus'] %}
{# about the course rather than its content; calendar rows still title and place sessions #}
{% set ocw_skipped_sections = ['calendar', 'related-resources', 'instructor-insights', 'download', 'resource-index'] %}
{% set ocw_transcript_extensions = ['.srt', '.vtt', '.webvtt'] %}
{% set ocw_resource_href = "'href=\"/(courses/[^/\"]+/resources/[^/\"#?]+)'" %}
{# a table column of answer keys: "Solutions", "Exam Solutions", "Answer Key"; not "Problem Sets and Solutions" #}
{% set ocw_solution_column = "'^((problem sets?|psets?|exams?|quiz(zes)?|labs?|homeworks?|assignments?|practice exams?|exam review|sample|recitations?|midterms?|finals?|final exam) )?(solutions?|answers?|answer keys?|keys?|solns?)$'" %}
{% set ocw_table_cell = "'(?s)<t[dh](?:\\s[^>]*)?>(.*?)</t[dh]>'" %}

with content as (
    select *
    from {{ ref('dim_course_content') }}
    where
        is_latest
        and platform in ('mitxonline', 'mitxpro', 'residential')
        {% if courserun_filter %}and courserun_readable_id in {{ courserun_filter }}{% endif %}
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
        {% if courserun_filter %}and courserun_readable_id in {{ courserun_filter }}{% endif %}
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
        {% if courserun_filter %}and courserun_readable_id in {{ courserun_filter }}{% endif %}
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
        {% if courserun_filter %}and documents.courserun_readable_id in {{ courserun_filter }}{% endif %}
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
        {% if courserun_filter %}and courserun_readable_id in {{ courserun_filter }}{% endif %}
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
    from content
    where block_category = 'sequential'
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

, unscoped_documents as (
    select documents.*
    from documents
    left join excluded_paths
        on
            documents.platform = excluded_paths.platform
            and documents.courserun_readable_id = excluded_paths.courserun_readable_id
            and documents.content_file_path = excluded_paths.content_file_path
    where excluded_paths.content_file_path is null
)

, context_sources as (
    select
        platform
        , courserun_readable_id
        , 'context' as scope_id
        , row_number() over (
            partition by platform, courserun_readable_id order by content_file_path
        ) as source_order
        , content_file_path as source_ref
        , concat({{ regexp_extract_or_null('content_file_path', "'^([a-z]+)/'", 1) }}, '_page') as source_kind
        , content_file_path as source_title
        , content_file_text as source_text
    from unscoped_documents
    where
        content_file_extension = '.html'
        and {{ regexp_extract_or_null('content_file_path', "'^([a-z]+)/'", 1) }} in ('about', 'tabs', 'info')
)

, material_sources as (
    select
        platform
        , courserun_readable_id
        , 'materials' as scope_id
        , row_number() over (
            partition by platform, courserun_readable_id order by content_file_path
        ) as source_order
        , content_file_path as source_ref
        , 'document' as source_kind
        , {{ regexp_replace_all('content_file_path', "'^static/'", "''") }} as source_title
        , content_file_text as source_text
    from unscoped_documents
    where
        content_file_path like 'static/%'
        and content_file_extension in ('{{ document_extensions | join("', '") }}')
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
        {% if ocw_course_filter %}and course_slug in {{ ocw_course_filter }}{% endif %}
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
        -- the resource's directory name, without the -1 OCW adds to a copy or a _pdf suffix
        , {{ regexp_replace_all(
            regexp_replace_all(regexp_extract_or_null('files.content_file_key', "'([^/]+)/$'", 1), "'-[0-9]+$'", "''")
            , "'[_-]pdf$'", "''"
        ) }} as file_name
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
            content_tags like '%Solutions%'
            or (
                -- a lecture video titled "... Iterative Solutions" is not an answer key
                content_type != 'video'
                and coalesce(file_extension, '') not in ('{{ ocw_transcript_extensions | join("', '") }}')
                and content_tags not like '%Videos%'
                and not {{ regexp_like('title_lower', "'(transcript|captions)$'") }}
                and (
                    -- a section page's opening lists its problem sets and their solutions
                    {{ courseware_answer_key(
                        'file_name', 'title_lower', "case when content_type != 'page' then text_head end", 'answer_markers'
                    ) }}
                    -- a page of "Answer:" entries on homework, e.g. virtual office hours
                    or (
                        content_type = 'page'
                        and coalesce(answer_markers, 0) >= 3
                        and {{ regexp_like('text_head', "'problem sets?|psets?|homework|exams?|quiz|midterm'") }}
                    )
                )
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
            else 'unit'
        end as page_role
    from ocw_files as files
    left join ocw_page_children as children
        on files.content_file_key = children.page_key
    where files.page_path is not null
)

-- A page with children is a unit, and so is a top-level section page; any other page
-- belongs to its parent's unit.
, ocw_unit_pages as (
    select
        *
        , case when has_children or depth = 1 then page_key else parent_key end as unit_key
        , depth = 1 and not has_children as is_flat_section
    from ocw_pages
    where page_role = 'unit'
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

, ocw_page_links as (
    select
        page_hrefs.courserun_readable_id
        , page_hrefs.page_key
        , page_hrefs.page_role
        , links.link_index
        , concat({{ element_at_array('page_hrefs.hrefs', 'links.link_index') }}, '/') as target_key
    from ocw_page_hrefs as page_hrefs
    cross join {{ unnest_sequence(array_length('page_hrefs.hrefs'), 'links', 'link_index') }}
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
        on files.content_file_key = solution_resources.content_file_key
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
        on files.content_file_key = solution_resources.content_file_key
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

, ocw_session_links as (
    select
        session_hrefs.courserun_readable_id
        , session_hrefs.page_key
        , session_hrefs.scope_id
        , session_hrefs.table_index * 1000000 + session_hrefs.row_index * 1000 + links.link_index as link_order
        , concat({{ element_at_array('session_hrefs.hrefs', 'links.link_index') }}, '/') as target_key
    from ocw_session_hrefs as session_hrefs
    cross join {{ unnest_sequence(array_length('session_hrefs.hrefs'), 'links', 'link_index') }}
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
            when units.depth = 1 then units.title
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
        on page_links.target_key = resources.content_file_key
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
        on unit_links.target_key = resources.content_file_key

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
        on session_links.target_key = resources.content_file_key

    union all

    -- documents no read page links; what only a skipped page links counts as unlinked
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
        select distinct target_key
        from ocw_page_links
        where page_role not in ('skipped', 'solution')
    ) as linked
        on resources.content_file_key = linked.target_key
    left join (select distinct target_key from ocw_session_links) as session_linked
        on resources.content_file_key = session_linked.target_key
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
        order by scoped_sources.source_order, scoped_sources.source_ref
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
