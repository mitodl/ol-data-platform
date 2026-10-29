{#
    One row per scope part the extractor's latest read of a run expects, with
    whether an extraction covers it. A part with no extraction has no row in
    int__courseware__extractions, so a scope that grew to two parts whose part 2
    failed shows up only here. has_current_input_extraction is false both for that
    and for a part whose changed text, or for v2 changed course header, failed to
    extract (its older extraction may still be current). Runs the extractor never recorded a read for are not listed.
    Which claims are current is decided in int__courseware__extractions, not here.
#}
with expected as (
    select * from {{ ref('int__courseware__latest_scope_inputs') }}
)

, extractions as (
    select * from {{ ref('int__courseware__extractions') }}
)

select
    expected.platform
    , expected.courserun_readable_id
    , expected.scope_id
    , expected.scope_part
    , expected.scope_part_count
    , expected.observed_at
    , count(extractions.extraction_key) as extraction_count
    -- a run the allowlist does not cover (null) counts every version
    , count(case when coalesce(extractions.is_approved_version, true) then extractions.extraction_key end) > 0
    as has_approved_extraction
    , count(
        case
            when coalesce(extractions.is_approved_version, true) and extractions.matches_current_input
                then extractions.extraction_key
        end
    ) > 0 as has_current_input_extraction
from expected
left join extractions
    on
        expected.platform = extractions.platform
        and expected.courserun_readable_id = extractions.courserun_readable_id
        and expected.scope_id = extractions.scope_id
        and expected.scope_part = extractions.scope_part
group by
    expected.platform
    , expected.courserun_readable_id
    , expected.scope_id
    , expected.scope_part
    , expected.scope_part_count
    , expected.observed_at
