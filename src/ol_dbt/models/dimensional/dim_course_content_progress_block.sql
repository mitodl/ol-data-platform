-- Grain: one row per (platform, course run, block) that counts toward a learner's progress
-- through the run's current course structure. Read with afact_learner_courserun_content_progress,
-- which counts a learner's completed units against these.
-- Progress is counted in units, as the Open edX progress page does: a unit is a vertical
-- directly under a sequential, and it is complete when every block in it that Open edX
-- records completion on is complete. vertical_block_id is that unit.
-- A block counts when Open edX records completion on it: every block that is not a
-- container, and not a discussion, which Open edX excludes from completion. The container
-- list is a deny list so a newly installed XBlock type counts without a change here.
-- Blocks under a staff-only chapter, sequential or vertical are left out. Staff-only is
-- checked on the block, its parent, its nearest vertical, its unit, and its sequential and
-- chapter. Other per-learner visibility (content groups, release dates, A/B split_test
-- branches) is not modelled, so a unit can hold blocks one learner is never shown.
-- A randomized pool (library_content, itembank) shows each learner max_count of its
-- children, so the pool is one progress item worth max_count and its children share it.
-- Every other block is its own item worth 1.
-- Limited to the platforms afact_learner_courserun_content_progress has completions for.
-- edX.org has a course structure and no completion records, so a total there would report
-- every edX.org learner at 0.
{% set container_categories = [
    'course', 'chapter', 'sequential', 'vertical', 'library_content', 'itembank', 'split_test', 'conditional'
] %}
{% set pool_categories = ['library_content', 'itembank'] %}
{% set completion_platforms = ['mitxonline', 'mitxpro', 'residential'] %}

with content as (
    select
        platform
        , courserun_readable_id
        , block_id
        , parent_block_id
        , block_category
        , block_metadata
        , chapter_block_id
        , sequential_block_id
        -- block_index is a depth-first order, so the last vertical seen is the block's own.
        , {{ last_value_ignore_nulls(
            "case when block_category = 'vertical' then block_id end"
          ) }} over (
            partition by platform, courserun_readable_id
            order by block_index
            rows between unbounded preceding and current row
        ) as nearest_vertical_block_id
        -- A vertical nested in a split_test or conditional is not a unit: its blocks belong
        -- to the vertical that sits under the sequential.
        , {{ last_value_ignore_nulls(
            "case when block_category = 'vertical' and parent_block_id = sequential_block_id then block_id end"
          ) }} over (
            partition by platform, courserun_readable_id, sequential_block_id
            order by block_index
            rows between unbounded preceding and current row
        ) as vertical_block_id
    from {{ ref('dim_course_content') }}
    where is_latest
      and platform in ('{{ completion_platforms | join("', '") }}')
)

, staff_only_blocks as (
    select
        platform
        , courserun_readable_id
        , block_id
    from content
    where {{ json_query_string('block_metadata', "'$.visible_to_staff_only'") }} = 'true'
)

-- visible_to_staff_only is inherited, and the structure only records it where it was set.
, hidden_blocks as (
    select distinct
        content.platform
        , content.courserun_readable_id
        , content.block_id
    from content
    inner join staff_only_blocks
        on content.platform = staff_only_blocks.platform
        and content.courserun_readable_id = staff_only_blocks.courserun_readable_id
        and staff_only_blocks.block_id in (
            content.block_id
            , content.parent_block_id
            , content.nearest_vertical_block_id
            , content.vertical_block_id
            , content.sequential_block_id
            , content.chapter_block_id
        )
)

, pools as (
    select
        platform
        , courserun_readable_id
        , block_id
        -- The structure records max_count only when it differs from the Open edX default of 1.
        -- Trino reads a JSON null as the string 'null'.
        , coalesce(
            cast(nullif({{ json_query_string('block_metadata', "'$.max_count'") }}, 'null') as integer), 1
        ) as max_count
    from content
    where block_category in ('{{ pool_categories | join("', '") }}')
)

, progress_blocks as (
    select
        content.platform
        , content.courserun_readable_id
        , content.block_id
        , content.block_category
        , content.chapter_block_id
        , content.sequential_block_id
        , content.vertical_block_id
        , coalesce(pools.block_id, content.block_id) as progress_item_block_id
        , pools.block_id is not null as is_in_pool
        , pools.max_count
    from content
    left join pools
        on content.platform = pools.platform
        and content.courserun_readable_id = pools.courserun_readable_id
        and content.parent_block_id = pools.block_id
    left join hidden_blocks
        on content.platform = hidden_blocks.platform
        and content.courserun_readable_id = hidden_blocks.courserun_readable_id
        and content.block_id = hidden_blocks.block_id
    where content.block_category not in ('{{ container_categories | join("', '") }}', 'discussion')
      and hidden_blocks.block_id is null
      and content.vertical_block_id is not null
)

, items as (
    select
        platform
        , courserun_readable_id
        , progress_item_block_id
        -- A negative max_count shows every child, and a pool cannot show more than it holds.
        , case
            when not max(is_in_pool) then 1
            when max(max_count) < 0 then count(*)
            else least(max(max_count), count(*))
        end as progress_item_weight
    from progress_blocks
    group by
        platform
        , courserun_readable_id
        , progress_item_block_id
)

, dim_course_run as (
    select courserun_pk, courserun_readable_id, platform
    from {{ ref('dim_course_run') }}
    where is_current = true
)

select
    dim_course_run.courserun_pk as courserun_fk
    , progress_blocks.platform
    , progress_blocks.courserun_readable_id
    , progress_blocks.block_id
    , progress_blocks.block_category
    , progress_blocks.chapter_block_id
    , progress_blocks.sequential_block_id
    , progress_blocks.vertical_block_id
    , progress_blocks.progress_item_block_id
    , items.progress_item_weight
from progress_blocks
inner join items
    on progress_blocks.platform = items.platform
    and progress_blocks.courserun_readable_id = items.courserun_readable_id
    and progress_blocks.progress_item_block_id = items.progress_item_block_id
left join dim_course_run
    on progress_blocks.platform = dim_course_run.platform
    and progress_blocks.courserun_readable_id = dim_course_run.courserun_readable_id
