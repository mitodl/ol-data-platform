with page_engagement as (
    select * from {{ ref('afact_course_page_engagement') }}
)

, dim_user as (
    select * from {{ ref('dim_user') }}
)

, course_runs as (
    select
        course_title
        , courserun_readable_id
    from {{ ref('int__combined__course_runs') }}
)

, unit_blocks as (
    select * from {{ ref('dim_course_content') }}
    where block_category = 'vertical'
    and is_latest = true
)

, subsection_blocks as (
    select * from {{ ref('dim_course_content') }}
    where is_latest = true
    and block_category = 'sequential'
)

, section_blocks as (
    select * from {{ ref('dim_course_content') }}
    where is_latest = true
    and block_category = 'chapter'
)

select
    dim_user.email as user_email
    , dim_user.full_name
    , page_engagement.platform
    , page_engagement.courserun_readable_id
    , page_engagement.num_of_views
    , unit_blocks.block_title as unit_title
    , subsection_blocks.block_title as subsection_title
    , subsection_blocks.block_index as subsection_block_index
    , section_blocks.block_title as section_title
    , section_blocks.block_index as section_block_index
    , course_runs.course_title
from page_engagement
inner join unit_blocks
    on page_engagement.block_fk = unit_blocks.block_id
inner join subsection_blocks
    on page_engagement.sequential_block_fk = subsection_blocks.block_id
inner join section_blocks
    on page_engagement.chapter_block_fk = section_blocks.block_id
inner join dim_user
    on (
        (page_engagement.platform = 'mitxonline'
            and page_engagement.openedx_user_id = dim_user.mitxonline_openedx_user_id)
        or (page_engagement.platform = 'edxorg'
            and page_engagement.openedx_user_id = dim_user.edxorg_openedx_user_id)
        or (page_engagement.platform = 'mitxpro'
            and page_engagement.openedx_user_id = dim_user.mitxpro_openedx_user_id)
        or (page_engagement.platform = 'residential'
            and page_engagement.openedx_user_id = dim_user.residential_openedx_user_id)
    )
left join course_runs
    on page_engagement.courserun_readable_id = course_runs.courserun_readable_id
