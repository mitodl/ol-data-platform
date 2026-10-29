{#
    Every file int__ocw__content_files publishes for the OCW courses
    var('courseware_ocw_courses') selects, with what int__courseware__extraction_sources
    scopes them by: whether the file is an answer key, its section and parent page,
    and for a page, its role and the unit it belongs to. The rules are described there.
    It is a table of its own because that model reads these files from many places,
    and Trino plans every reference to a CTE again (QUERY_HAS_TOO_MANY_STAGES).
#}
{% set ocw_context_sections = ['syllabus'] %}
{# about the course rather than its content; calendar rows still title and place sessions #}
{% set ocw_skipped_sections = ['calendar', 'related-resources', 'instructor-insights', 'download', 'resource-index'] %}
{% set ocw_textbook_section = "'^((open|online)-)?textbooks?(-|$)'" %}

with ocw_courses as (
    select
        course_slug
        , courserun_readable_id
    from {{ ref('int__ocw__live_courses') }}
    where
        courserun_readable_id is not null
        and course_slug in {{ courseware_ocw_course_filter() }}
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

select
    files.courserun_readable_id
    , files.course_slug
    , files.content_file_key
    , files.content_type
    , files.title
    , files.file_extension
    , files.page_path
    , files.section
    , files.parent_key
    , files.is_solution
    , files.body
    , pages.depth
    , pages.has_children
    , pages.page_role
    , unit_pages.unit_key
    , unit_pages.is_flat_section
from ocw_files as files
left join ocw_pages as pages
    on files.content_file_key = pages.page_key
left join ocw_unit_pages as unit_pages
    on files.content_file_key = unit_pages.page_key
