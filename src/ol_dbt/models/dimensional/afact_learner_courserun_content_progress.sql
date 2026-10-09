-- Grain: one row per (platform, user, course run) with at least one completed block that is
-- still in the run's current structure. A learner with none has no row.
-- The completion is Open edX's own (completion_blockcompletion), so it covers ungraded
-- content and needs no choice between graded, attempted and visited. edX.org and Bootcamps
-- have no such table and never appear here.
-- content_units_completed counts units (verticals) the way the Open edX progress page
-- does: a unit is complete when every block in it that counts is complete. It is 0 for a
-- learner who has completed blocks and finished no unit.
-- Completions are counted against dim_course_content_progress_block, so a block removed
-- from the course since the learner completed it no longer counts, and
-- content_units_completed cannot exceed the run's total.
-- Rebuilt in full (the dimensional default): user_fk can re-key and the structure a
-- completion is counted against changes.
with completions as (
    select
        'mitxonline' as platform
        , user_id as openedx_user_id
        , courserun_readable_id
        , block_fk
    from {{ ref('stg__mitxonline__openedx__blockcompletion') }}
    where block_completed >= 1

    union all

    select
        'mitxpro' as platform
        , user_id as openedx_user_id
        , courserun_readable_id
        , block_fk
    from {{ ref('stg__mitxpro__openedx__blockcompletion') }}
    where block_completed >= 1

    union all

    select
        'residential' as platform
        , user_id as openedx_user_id
        , courserun_readable_id
        , block_fk
    from {{ ref('stg__mitxresidential__openedx__blockcompletion') }}
    where block_completed >= 1
)

-- One user per Open edX id, as tfact_studentmodule_problems does: the same id can sit on
-- more than one dim_user row, and joining both would credit one completion to two learners.
, users as (
    select
        'mitxonline' as platform
        , mitxonline_openedx_user_id as openedx_user_id
        , min(user_pk) as user_pk
    from {{ ref('dim_user') }}
    where mitxonline_openedx_user_id is not null
    group by mitxonline_openedx_user_id

    union all

    select
        'mitxpro' as platform
        , mitxpro_openedx_user_id as openedx_user_id
        , min(user_pk) as user_pk
    from {{ ref('dim_user') }}
    where mitxpro_openedx_user_id is not null
    group by mitxpro_openedx_user_id

    union all

    select
        'residential' as platform
        , residential_openedx_user_id as openedx_user_id
        , min(user_pk) as user_pk
    from {{ ref('dim_user') }}
    where residential_openedx_user_id is not null
    group by residential_openedx_user_id
)

, item_completions as (
    select
        completions.platform
        , completions.openedx_user_id
        , progress_blocks.courserun_fk
        , progress_blocks.courserun_readable_id
        , progress_blocks.vertical_block_id
        , progress_blocks.progress_item_block_id
        , max(progress_blocks.progress_item_weight) as progress_item_weight
        , count(distinct completions.block_fk) as blocks_completed
    from completions
    inner join {{ ref('dim_course_content_progress_block') }} as progress_blocks
        on completions.platform = progress_blocks.platform
        and completions.courserun_readable_id = progress_blocks.courserun_readable_id
        and completions.block_fk = progress_blocks.block_id
    where progress_blocks.courserun_fk is not null
    group by
        completions.platform
        , completions.openedx_user_id
        , progress_blocks.courserun_fk
        , progress_blocks.courserun_readable_id
        , progress_blocks.vertical_block_id
        , progress_blocks.progress_item_block_id
)

, unit_completions as (
    select
        platform
        , openedx_user_id
        , courserun_fk
        , courserun_readable_id
        , vertical_block_id
        -- A learner shown more of a randomized pool than its max_count (the pool was
        -- re-drawn) still completes the pool once.
        , sum(least(blocks_completed, progress_item_weight)) as weight_completed
    from item_completions
    group by
        platform
        , openedx_user_id
        , courserun_fk
        , courserun_readable_id
        , vertical_block_id
)

-- A pool is one item however many of its blocks dim_course_content_progress_block lists,
-- so the items are deduplicated before their weights are summed.
, unit_items as (
    select distinct
        platform
        , courserun_readable_id
        , vertical_block_id
        , progress_item_block_id
        , progress_item_weight
    from {{ ref('dim_course_content_progress_block') }}
)

, unit_weights as (
    select
        platform
        , courserun_readable_id
        , vertical_block_id
        , sum(progress_item_weight) as unit_weight
    from unit_items
    group by
        platform
        , courserun_readable_id
        , vertical_block_id
)

, learner_completions as (
    select
        unit_completions.platform
        , unit_completions.openedx_user_id
        , unit_completions.courserun_fk
        , unit_completions.courserun_readable_id
        , sum(
            case when unit_completions.weight_completed >= unit_weights.unit_weight then 1 else 0 end
        ) as content_units_completed
    from unit_completions
    inner join unit_weights
        on unit_completions.platform = unit_weights.platform
        and unit_completions.courserun_readable_id = unit_weights.courserun_readable_id
        and unit_completions.vertical_block_id = unit_weights.vertical_block_id
    group by
        unit_completions.platform
        , unit_completions.openedx_user_id
        , unit_completions.courserun_fk
        , unit_completions.courserun_readable_id
)

select
    users.user_pk as user_fk
    , learner_completions.courserun_fk
    , learner_completions.platform
    , learner_completions.courserun_readable_id
    , learner_completions.content_units_completed
from learner_completions
inner join users
    on learner_completions.platform = users.platform
    and learner_completions.openedx_user_id = users.openedx_user_id
