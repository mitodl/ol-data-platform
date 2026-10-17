{#
    One row per scope part the extractor's latest read of a run expects, with
    whether a promoted extraction covers it. A part with no extraction has no row in
    int__courseware__extractions, so a scope that grew to two parts whose part 2
    failed only shows up here. has_current_input_extraction is false both for that
    and for a part whose changed text failed to extract (its older extraction is
    still current). Runs the extractor never recorded a read for are not listed.
    Which claims are current is decided in int__courseware__extractions, not here.
#}
with expected as (
    select * from {{ ref('int__courseware__latest_scope_inputs') }}
)

, promoted as (
    select * from {{ ref('int__courseware__extractions') }}
    where is_promoted_version
)

select
    expected.platform
    , expected.courserun_readable_id
    , expected.scope_id
    , expected.scope_part
    , expected.scope_part_count
    , expected.observed_at
    , count(promoted.extraction_key) as promoted_extraction_count
    , count(promoted.extraction_key) > 0 as has_promoted_extraction
    , count(case when promoted.matches_current_input then 1 end) > 0 as has_current_input_extraction
from expected
left join promoted
    on
        expected.platform = promoted.platform
        and expected.courserun_readable_id = promoted.courserun_readable_id
        and expected.scope_id = promoted.scope_id
        and expected.scope_part = promoted.scope_part
group by
    expected.platform
    , expected.courserun_readable_id
    , expected.scope_id
    , expected.scope_part
    , expected.scope_part_count
    , expected.observed_at
