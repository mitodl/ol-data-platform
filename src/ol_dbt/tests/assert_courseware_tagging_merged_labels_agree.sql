{{ config(
    severity="warn"
    , enabled=courseware_local_tables_enabled("courseware_tagging_in_warehouse")
) }}

-- Tagging through a label that resolution merged into a concept spreads that merge to
-- every occurrence of the label. If merges were wrong at a meaningful rate, tags through
-- merged labels would agree with the extractor (a claim naming the concept in the same
-- scope) less often than tags through labels that were never merged. A row here means
-- they do, by more than 0.1 over at least 50 compared sources each; review the merged
-- phrases with the most tagged sources in int__courseware__concept_tagging_fan_out.
with phrases as (
    select * from {{ ref('int__courseware__concept_tagging_fan_out') }}
    where not is_ambiguous and compared_source_count > 0
)

, agreement as (
    select
        sum(case when is_merged_label then compared_source_count else 0 end) as merged_sources
        , sum(case when not is_merged_label then compared_source_count else 0 end) as unmerged_sources
        , sum(case when is_merged_label then compared_source_count * scope_agreement_rate else 0 end)
        / nullif(sum(case when is_merged_label then compared_source_count else 0 end), 0) as merged_agreement
        , sum(case when not is_merged_label then compared_source_count * scope_agreement_rate else 0 end)
        / nullif(sum(case when not is_merged_label then compared_source_count else 0 end), 0) as unmerged_agreement
    from phrases
)

select *
from agreement
where
    merged_sources >= 50
    and unmerged_sources >= 50
    and merged_agreement < unmerged_agreement - 0.1
