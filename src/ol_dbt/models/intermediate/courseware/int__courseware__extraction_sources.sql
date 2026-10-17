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
#}
{% set courseruns = var('courseware_courseruns', []) %}
{% set honored_exclusions = ['unreferenced_static', 'unreachable_tab', 'legacy_announcements', 'non_content', 'course_settings'] %}
{% set document_extensions = ['.pdf', '.pptx', '.ppt', '.docx', '.doc', '.txt', '.md'] %}

{% set courserun_filter = "('" ~ courseruns | join("', '") ~ "')" if courseruns else none %}

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
        , coursestructure_xml_block_id as block_name
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
    inner join latest_xml_blocks
        on
            leaf_blocks.platform = latest_xml_blocks.platform
            and leaf_blocks.courserun_readable_id = latest_xml_blocks.courserun_readable_id
            and leaf_blocks.block_category = latest_xml_blocks.block_type
            and leaf_blocks.block_name = latest_xml_blocks.block_name
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
)

, course_titles as (
    select
        platform
        , courserun_readable_id
        , block_title as course_title
    from content
    where block_category = 'course'
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
