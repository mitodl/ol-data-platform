{{ config(severity="warn") }}

-- bridge_concept_relation cuts the lowest-ranked edge of every broader cycle in one pass
-- over all broader edges, so where cycles overlap it can cut an edge for a cycle that a
-- cut above it already broke. This runs one more pass over the edges left: a row is a
-- hierarchy_cycle edge that no cycle of surviving higher-ranked edges closes. Every
-- edge an exact pass would keep is here, so the row count is an upper bound on the
-- over-pruning; rows mean the one-pass rule is costing edges.
with hierarchy_edges as (
    {{ courseware_hierarchy_edges(
        "(select from_concept_fk as from_concept_id, to_concept_fk as to_concept_id, verdict_support,"
        ~ " contrary_verdict_support from " ~ ref('bridge_concept_relation')
        ~ " where relation = 'broader' and is_adjudicated)"
    ) }}
)

, cut_edges as (
    select hierarchy_edges.*
    from hierarchy_edges
    inner join {{ ref('bridge_concept_relation') }} as bridge
        on
            hierarchy_edges.from_concept_id = bridge.from_concept_fk
            and hierarchy_edges.to_concept_id = bridge.to_concept_fk
            and bridge.relation = 'broader'
    where bridge.support_rule_failure in ('hierarchy_cycle', 'hierarchy_cycle_unresolved')
)

, surviving_edges as (
    select hierarchy_edges.*
    from hierarchy_edges
    left join cut_edges
        on
            hierarchy_edges.from_concept_id = cut_edges.from_concept_id
            and hierarchy_edges.to_concept_id = cut_edges.to_concept_id
    where cut_edges.from_concept_id is null
)

{{ courseware_hierarchy_cycle_cuts('second_pass_cuts', 'cut_edges', 'surviving_edges') }}

select
    cut_edges.from_concept_id as from_concept_fk
    , cut_edges.to_concept_id as to_concept_fk
    , cut_edges.cycle_rank
from cut_edges
left join second_pass_cuts
    on
        cut_edges.from_concept_id = second_pass_cuts.from_concept_id
        and cut_edges.to_concept_id = second_pass_cuts.to_concept_id
where second_pass_cuts.from_concept_id is null
