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

    An extraction is retired, and never current, when its scope part is gone from
    the course: the run is in the sources but the scope is not (a deleted
    sequential), or the part is past the part count of the scope's newest promoted
    extraction (a scope that shrank from two parts to one). A run the sources do not
    list at all is left as it is, since var('courseware_courseruns') may only have
    left it out.
#}
with extractions as (
    select * from {{ source('courseware_intermediate', 'courseware_claim_extraction') }}
)

, promotions as (
    select * from {{ ref('courseware_extraction_promotions') }}
)

, current_scopes as (
    select distinct
        platform
        , courserun_readable_id
        , scope_id
        , scope_position
    from {{ ref('int__courseware__extraction_sources') }}
)

, current_runs as (
    select distinct
        platform
        , courserun_readable_id
    from current_scopes
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
        , extractions.scope_part_count
        , coalesce(current_scopes.scope_position, extractions.scope_position) as scope_position
        , current_runs.courserun_readable_id is not null
        and current_scopes.scope_id is null as is_deleted_scope
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
    left join current_scopes
        on
            extractions.platform = current_scopes.platform
            and extractions.courserun_readable_id = current_scopes.courserun_readable_id
            and extractions.scope_id = current_scopes.scope_id
    left join current_runs
        on
            extractions.platform = current_runs.platform
            and extractions.courserun_readable_id = current_runs.courserun_readable_id
)

-- A changed split re-extracts every part, so the newest promoted extraction carries
-- the scope's current part count.
, current_part_counts as (
    select
        platform
        , courserun_readable_id
        , scope_id
        , scope_part_count
    from (
        select
            platform
            , courserun_readable_id
            , scope_id
            , scope_part_count
            , row_number() over (
                partition by platform, courserun_readable_id, scope_id
                order by extracted_at desc, scope_part_count desc
            ) as newest_rank
        from versioned
        where is_promoted_version
    ) as promoted
    where newest_rank = 1
)

, ranked as (
    select
        versioned.*
        , versioned.is_deleted_scope
        or versioned.scope_part > current_part_counts.scope_part_count as is_retired
        , row_number() over (
            partition by versioned.platform, versioned.courserun_readable_id, versioned.scope_id, versioned.scope_part
            order by case when versioned.is_promoted_version then 0 else 1 end, versioned.extracted_at desc
        ) as version_rank
        , max(case when versioned.is_promoted_version then 1 else 0 end) over (
            partition by versioned.platform, versioned.courserun_readable_id, versioned.scope_id, versioned.scope_part
        ) = 1 as has_promoted_extraction
    from versioned
    left join current_part_counts
        on
            versioned.platform = current_part_counts.platform
            and versioned.courserun_readable_id = current_part_counts.courserun_readable_id
            and versioned.scope_id = current_part_counts.scope_id
)

select
    extraction_key
    , platform
    , courserun_readable_id
    , scope_id
    , scope_part
    , scope_position
    , is_promoted_version
    , coalesce(is_retired, false) as is_retired
    , is_promoted_version and version_rank = 1 and not coalesce(is_retired, false) as is_current
    , has_promoted_extraction
from ranked
