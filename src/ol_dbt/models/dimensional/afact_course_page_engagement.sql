with vertical_structure as (
    select
        courserun_readable_id
        , block_id
        , block_index
        , sequential_block_id as sequential_block_fk
        , chapter_block_id as chapter_block_fk
    from {{ ref('dim_course_content') }}
    where
        block_category = 'vertical'
        and is_latest = true
)

, page_navigation_events as (
    select
        platform
        , openedx_user_id
        , courserun_readable_id
        , block_fk
        , count(*) as num_of_views
        , max(event_timestamp) as last_view_timestamp
    from {{ ref('tfact_course_navigation_events') }}
    where block_fk is not null
    group by platform, courserun_readable_id, openedx_user_id, block_fk
)

, combined as (
    select
        page_navigation_events.platform
        , page_navigation_events.openedx_user_id
        , page_navigation_events.courserun_readable_id
        , page_navigation_events.block_fk
        , page_navigation_events.num_of_views
        , page_navigation_events.last_view_timestamp
        , vertical_structure.sequential_block_fk
        , vertical_structure.chapter_block_fk
    from page_navigation_events
    left join vertical_structure
        on page_navigation_events.block_fk = vertical_structure.block_id
)

select
    platform
    , openedx_user_id
    , courserun_readable_id
    , block_fk
    , num_of_views
    , sequential_block_fk
    , chapter_block_fk
    , last_view_timestamp
from combined
