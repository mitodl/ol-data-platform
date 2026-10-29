{#
    Concept-to-concept edges with the evidence behind them, one row per
    (from, to, relation). This is the graph's edge table and the input to
    ml.lib.courseware.graph.

    relation_family groups relations by what they mean for a learner:
      hierarchy    broader      from is a kind or part of to
      sequence     builds_on    from is learned on top of to
      practice     practiced_with  learning from involves using the tool to
      equivalence  close        nearly interchangeable (exact matches are merged
                                into one concept, not linked)
      association  complements  taught or assessed together
                   related      connected, per adjudication only

    Support columns let consumers pick their own threshold rather than this model
    deciding one:
      courserun_support         distinct course runs with course evidence
      reverse_courserun_support builds_on only: runs where the opposite direction
                                was observed; direction_consistency is
                                support / (support + reverse)
      co_taught_lift            complements only: observed co-taught unit count
                                over the count expected if the two were taught
                                independently across all taught units
    first_observed_at and last_observed_at show how the edge has evolved; an edge
    whose evidence disappears (an extraction version replaced) drops out on the
    next build.
#}
with evidence as (
    select * from {{ ref('int__courseware__concept_relation_evidence') }}
)

, mentions as (
    select * from {{ ref('int__courseware__resolved_concept_mentions') }}
)

, taught_units as (
    select distinct
        platform
        , courserun_readable_id
        , scope_id
        , concept_id
    from mentions
    where
        claim_kind = 'concept'
        and teaching_status = 'taught'
        and scope_kind = 'unit'
)

, unit_totals as (
    select count(distinct concat(platform, '|', courserun_readable_id, '|', scope_id)) as taught_unit_count
    from taught_units
)

, concept_unit_counts as (
    select
        concept_id
        , count(*) as taught_unit_count
    from taught_units
    group by concept_id
)

, edges as (
    select
        from_concept_id
        , to_concept_id
        , relation
        , count(
            distinct case
                when evidence_source != 'adjudication'
                    then concat(platform, '|', courserun_readable_id)
            end
        ) as courserun_support
        , sum(scope_count) as scope_support
        , sum(case when evidence_source = 'co_taught' then scope_count else 0 end) as co_taught_unit_count
        , max(case when evidence_source = 'adjudication' then 1 else 0 end) = 1 as is_adjudicated
        , max(case when evidence_source = 'course_sequence' then 1 else 0 end) = 1 as has_sequence_evidence
        , max(case when evidence_source = 'co_taught' then 1 else 0 end) = 1 as has_co_taught_evidence
        , max(case when evidence_source = 'co_assessed' then 1 else 0 end) = 1 as has_co_assessed_evidence
        , min(observed_at) as first_observed_at
        , max(observed_at) as last_observed_at
    from evidence
    group by from_concept_id, to_concept_id, relation
)

select
    {{ dbt_utils.generate_surrogate_key(['edges.from_concept_id', 'edges.to_concept_id', 'edges.relation']) }}
    as concept_relation_pk
    , edges.from_concept_id as from_concept_fk
    , edges.to_concept_id as to_concept_fk
    , edges.relation
    , case edges.relation
        when 'broader' then 'hierarchy'
        when 'builds_on' then 'sequence'
        when 'practiced_with' then 'practice'
        when 'close' then 'equivalence'
        else 'association'
    end as relation_family
    , edges.courserun_support
    , edges.scope_support
    , coalesce(reverse_edges.courserun_support, 0) as reverse_courserun_support
    , case
        when edges.relation = 'builds_on'
            then cast(edges.courserun_support as double)
            / nullif(edges.courserun_support + coalesce(reverse_edges.courserun_support, 0), 0)
    end as direction_consistency
    , case
        when edges.relation = 'complements' and edges.co_taught_unit_count > 0
            then edges.co_taught_unit_count * cast(unit_totals.taught_unit_count as double)
            / (from_units.taught_unit_count * to_units.taught_unit_count)
    end as co_taught_lift
    , edges.is_adjudicated
    , edges.has_sequence_evidence
    , edges.has_co_taught_evidence
    , edges.has_co_assessed_evidence
    , edges.first_observed_at
    , edges.last_observed_at
    , 'proposed' as review_status
from edges
cross join unit_totals
left join edges as reverse_edges
    on
        edges.relation = 'builds_on'
        and reverse_edges.relation = 'builds_on'
        and edges.from_concept_id = reverse_edges.to_concept_id
        and edges.to_concept_id = reverse_edges.from_concept_id
left join concept_unit_counts as from_units
    on edges.from_concept_id = from_units.concept_id
left join concept_unit_counts as to_units
    on edges.to_concept_id = to_units.concept_id
