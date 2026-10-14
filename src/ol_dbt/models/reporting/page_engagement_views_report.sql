with page_engagement as (
    select * from {{ ref('afact_course_page_engagement') }}
)

, dim_user as (
    select * from {{ ref('dim_user') }}
)

-- dim_course_run and dim_course are SCD2 and can hold more than one is_current = true row per
-- key, so keep the latest. Runs with no dim_course row (all residential runs) use the run title.
, course_runs as (
    select
        courserun_readable_id
        , platform
        , course_title
    from (
        select
            dim_course_run.courserun_readable_id
            , dim_course_run.platform
            , coalesce(dim_course.course_title, dim_course_run.courserun_title) as course_title
            , row_number() over (
                partition by dim_course_run.platform, dim_course_run.courserun_readable_id
                order by
                    dim_course_run.effective_date desc nulls last
                    , dim_course.effective_date desc nulls last
            ) as _row_num
        from {{ ref('dim_course_run') }} as dim_course_run
        left join {{ ref('dim_course') }} as dim_course
            on
                dim_course_run.course_fk = dim_course.course_pk
                and dim_course.is_current = true
        where dim_course_run.is_current = true
    )
    where _row_num = 1
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
    , page_engagement.block_fk
    , page_engagement.openedx_user_id
    , max(page_engagement.num_of_views) as num_of_views
    , max(unit_blocks.block_title) as unit_title
    , max(subsection_blocks.block_title) as subsection_title
    , max(subsection_blocks.block_index) as subsection_block_index
    , max(section_blocks.block_title) as section_title
    , max(section_blocks.block_index) as section_block_index
    , max(course_runs.course_title) as course_title
from page_engagement
left join unit_blocks
    on page_engagement.block_fk = unit_blocks.block_id
left join subsection_blocks
    on page_engagement.sequential_block_fk = subsection_blocks.block_id
left join section_blocks
    on page_engagement.chapter_block_fk = section_blocks.block_id
left join dim_user
    on page_engagement.user_fk= dim_user.user_pk
left join course_runs
    on page_engagement.courserun_readable_id = course_runs.courserun_readable_id
    and page_engagement.platform = course_runs.platform
group by
    dim_user.email
    , dim_user.full_name
    , page_engagement.platform
    , page_engagement.courserun_readable_id
    , page_engagement.block_fk
    , page_engagement.openedx_user_id
