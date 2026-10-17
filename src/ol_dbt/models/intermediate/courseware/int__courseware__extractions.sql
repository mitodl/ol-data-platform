{#
    Which extraction of each scope part is current. A course run reads one promoted
    extractor version from courseware_extraction_promotions (the run's own row, else
    its platform's), so a trial run or a second model's comparison run never becomes
    current by finishing last. Within the promoted version, the extraction whose
    input matches the scope part as the extractor last read it wins
    (courseware_scope_input: same input_text_checksum and part count). Only Python
    computes that checksum (the sources block and the split by input budget), so the
    extractor records it for every scope part it reads, extracted or skipped. That
    is what makes a scope whose text goes A, B, then back to A current on A again,
    though A's extraction is the older one. With no match (a scope part never
    observed, or one whose new text is not extracted yet) the newest extraction
    wins.

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
    the course: a unit scope whose sequential the run's latest course structure
    (dim_course_content) no longer has, or a part past the part count of the
    scope's matching promoted extraction, else its newest (a scope that shrank from
    two parts to one).
    Deletion is read from the structure, not from extraction_sources, because a
    scope with no readable text this build (e.g. a failed Tika parse) is missing
    from the sources too and must keep its claims. Runs with no Open edX structure
    (OCW) and the context and materials scopes are never retired as deleted.
#}
with extractions as (
    select * from {{ source('courseware_intermediate', 'courseware_claim_extraction') }}
)

, promotions as (
    select * from {{ ref('courseware_extraction_promotions') }}
)

, scope_inputs as (
    select * from {{ source('courseware_intermediate', 'courseware_scope_input') }}
)

, latest_reads as (
    select
        platform
        , courserun_readable_id
        , max(observed_at) as observed_at
    from scope_inputs
    group by platform, courserun_readable_id
)

-- each run's latest read, which lists every scope part the extractor read
, latest_scope_inputs as (
    select scope_inputs.*
    from scope_inputs
    inner join latest_reads
        on
            scope_inputs.platform = latest_reads.platform
            and scope_inputs.courserun_readable_id = latest_reads.courserun_readable_id
            and scope_inputs.observed_at = latest_reads.observed_at
)

, current_scopes as (
    select distinct
        platform
        , courserun_readable_id
        , scope_id
        , scope_position
    from {{ ref('int__courseware__extraction_sources') }}
)

, structure as (
    select
        platform
        , courserun_readable_id
        , block_id
        , block_category
    from {{ ref('dim_course_content') }}
    where is_latest
)

, structure_runs as (
    select distinct
        platform
        , courserun_readable_id
    from structure
)

, structure_sequentials as (
    select distinct
        platform
        , courserun_readable_id
        , block_id as scope_id
    from structure
    where block_category = 'sequential'
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
        , extractions.scope_kind = 'unit'
        and structure_runs.courserun_readable_id is not null
        and structure_sequentials.scope_id is null as is_deleted_scope
        , extractions.extracted_at
        , coalesce(
            extractions.input_text_checksum = latest_scope_inputs.input_text_checksum
            and extractions.scope_part_count = latest_scope_inputs.scope_part_count
            , false
        ) as matches_current_input
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
    left join structure_runs
        on
            extractions.platform = structure_runs.platform
            and extractions.courserun_readable_id = structure_runs.courserun_readable_id
    left join structure_sequentials
        on
            extractions.platform = structure_sequentials.platform
            and extractions.courserun_readable_id = structure_sequentials.courserun_readable_id
            and extractions.scope_id = structure_sequentials.scope_id
    left join latest_scope_inputs
        on
            extractions.platform = latest_scope_inputs.platform
            and extractions.courserun_readable_id = latest_scope_inputs.courserun_readable_id
            and extractions.scope_id = latest_scope_inputs.scope_id
            and extractions.scope_part = latest_scope_inputs.scope_part
)

-- A changed split re-extracts every part, so the matching promoted extraction (else
-- the newest) carries the scope's current part count.
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
                order by
                    case when matches_current_input then 0 else 1 end
                    , extracted_at desc
                    , scope_part_count desc
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
            order by
                case when versioned.is_promoted_version then 0 else 1 end
                , case when versioned.matches_current_input then 0 else 1 end
                , versioned.extracted_at desc
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
