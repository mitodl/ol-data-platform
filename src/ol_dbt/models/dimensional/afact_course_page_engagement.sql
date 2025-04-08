with pages_viewed as (
    select
        user_id
        , courserun_readable_id
        , block_id
        , json_query(event_json, 'lax $.tab_count' omit quotes) as num_of_pages
        , sum(nullif(cast(ending_position as double),0) - cast(starting_position as double)) as num_of_pages_viewed
    from tfact_course_navigation_events
    group by user_id, block_id, event_json
)

, sections_viewed as (
    select
        user_id
        , count(distinct block_id) as num_of_sections_viewed
    from tfact_course_navigation_events
    where
        -- most of the events are one of these two types
        -- indicates a learner is done with a section
        event_type in ('edx.ui.lms.sequence.next_selected',
        -- indicates a learner is done with a section
        event_type = 'edx.ui.lms.sequence.previous_selected')
    group by user_id
)

, links_clicked as (
    select
        user_id,
        , courserun_readable_id
        , event_type
        , block_id
        , starting_position
        , ending_position
        , event_timestamp
        , event_json
    from tfact_course_navigation_events
    where event_type = 'link_clicked'
    group by user_id, block_id
)

, course_sections as (
    select
        courserun_readable_id
        , parent_block_id as section_block_id
        , parent.block_title as section_name
        , block_id as subsection_block_id
        , block_title as subsection_name
    from dim_course_content
    left join dim_course_content as parent
        on dim_course_content.parent_block_id = parent.block_id
    where block_category in
        ('course', 'chapter', 'sequential', 'vertical', 'discussion', 'html')
)

, combined as (
    select
        pages_viewed.user_id
        , pages_viewed.courserun_readable_id
        , user_course_videos.video_id as section_block_id
        , course_sections.section_name
        , course_sections.subsection_block_id
        , course_sections.subsection_name
        , pages_viewed.num_of_pages,
        , pages_viewed.num_of_pages_viewed
        , count(distinct course_sections.block_id) as num_of_sections
        , sections_viewed.num_of_sections_viewed
    from pages_viewed
    left join sections_viewed
        on pages_viewed.user_id = sections_viewed.user_id
        and pages_viewed.courserun_readable_id = sections_viewed.courserun_readable_id
    left join sections_viewed
        on pages_viewed.user_id = sections_viewed.user_id
        and pages_viewed.courserun_readable_id = sections_viewed.courserun_readable_id
    left join links_clicked
        on pages_viewed.user_id = links_clicked.user_id
        and pages_viewed.courserun_readable_id = links_clicked.courserun_readable_id
    left join course_sections
        on pages_viewed.block_id = course_sections.subsection_block_id
)

select distinct
    user_id,
    courserun_readable_id,
    section_block_id,
    section_name,
    subsection_block_id,
    subsection_name,
    num_of_pages,
    num_of_pages_viewed,
    -- todo: not sure if the tabs viewed or blocks navigated to count as "pages" in this context
    num_of_sections,
    num_of_sections_viewed
from combined
