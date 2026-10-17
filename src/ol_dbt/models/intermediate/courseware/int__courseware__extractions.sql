{#
    Which extraction of each scope part is current. A course run reads one promoted
    extractor version from courseware_extraction_promotions (the run's own row, else
    its platform's), so a trial run or a second model's comparison run never becomes
    current by finishing last. Within the promoted version the newest extraction of a
    scope part wins: a second one under the same version means the scope's text
    changed.

    A scope part with no extraction under its run's promoted version (or on a
    platform with no promotion) has no current extraction and drops out of the
    rollups; has_promoted_extraction is false on its rows so that is visible.

    scope_position is the scope's position in the course as it is now, from
    int__courseware__extraction_sources. The extraction key leaves position out,
    because reordering a course changes nothing a scope's call reads except the
    outline, so the position stored at extraction time goes stale. It is kept only
    for a scope the sources no longer list (or leave out, when var('courseware_courseruns')
    limits them).
#}
with extractions as (
    select * from {{ source('courseware_intermediate', 'courseware_claim_extraction') }}
)

, promotions as (
    select * from {{ ref('courseware_extraction_promotions') }}
)

, current_positions as (
    select distinct
        platform
        , courserun_readable_id
        , scope_id
        , scope_position
    from {{ ref('int__courseware__extraction_sources') }}
)

, runs as (
    select distinct
        platform
        , courserun_readable_id
    from extractions
)

, run_promotions as (
    select
        runs.platform
        , runs.courserun_readable_id
        , coalesce(run_promotion.extractor_id, platform_promotion.extractor_id) as extractor_id
        , coalesce(run_promotion.prompt_version, platform_promotion.prompt_version) as prompt_version
        , coalesce(run_promotion.schema_version, platform_promotion.schema_version) as schema_version
        , coalesce(run_promotion.model_id, platform_promotion.model_id) as model_id
        , coalesce(run_promotion.effort, platform_promotion.effort) as effort
    from runs
    left join promotions as run_promotion
        on
            runs.platform = run_promotion.platform
            and runs.courserun_readable_id = run_promotion.courserun_readable_id
    left join promotions as platform_promotion
        on
            runs.platform = platform_promotion.platform
            and platform_promotion.courserun_readable_id is null
)

, versioned as (
    select
        extractions.extraction_key
        , extractions.platform
        , extractions.courserun_readable_id
        , extractions.scope_id
        , extractions.scope_part
        , coalesce(current_positions.scope_position, extractions.scope_position) as scope_position
        , extractions.extracted_at
        , coalesce(
            extractions.extractor_id = run_promotions.extractor_id
            and extractions.prompt_version = run_promotions.prompt_version
            and extractions.schema_version = run_promotions.schema_version
            and extractions.model_id = run_promotions.model_id
            and extractions.effort = run_promotions.effort
            , false
        ) as is_promoted_version
    from extractions
    inner join run_promotions
        on
            extractions.platform = run_promotions.platform
            and extractions.courserun_readable_id = run_promotions.courserun_readable_id
    left join current_positions
        on
            extractions.platform = current_positions.platform
            and extractions.courserun_readable_id = current_positions.courserun_readable_id
            and extractions.scope_id = current_positions.scope_id
)

, ranked as (
    select
        *
        , row_number() over (
            partition by platform, courserun_readable_id, scope_id, scope_part
            order by case when is_promoted_version then 0 else 1 end, extracted_at desc
        ) as version_rank
        , max(case when is_promoted_version then 1 else 0 end) over (
            partition by platform, courserun_readable_id, scope_id, scope_part
        ) = 1 as has_promoted_extraction
    from versioned
)

select
    extraction_key
    , platform
    , courserun_readable_id
    , scope_id
    , scope_part
    , scope_position
    , is_promoted_version
    , is_promoted_version and version_rank = 1 as is_current
    , has_promoted_extraction
from ranked
