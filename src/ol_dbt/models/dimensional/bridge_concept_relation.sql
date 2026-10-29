{#
    Concept-to-concept edges with the evidence behind them, one row per
    (from, to, relation). This is the graph's edge table and the input to
    ml.lib.courseware.graph.

    Every row is a candidate relation aggregated from observations
    (int__courseware__concept_relation_evidence, which keeps the course, unit,
    source and extractor version of each), not an accepted assertion: review_status
    stays proposed until a review workflow exists.

    relation_family groups relations by what they mean for a learner:
      hierarchy    broader      from is a kind or part of to
      sequence     builds_on    units teaching from relied on to (an instructional
                                observation, not evidence that from requires to)
      practice     practiced_with  learning from involves using the tool to
      equivalence  close        nearly interchangeable (exact matches are merged
                                into one concept, not linked)
      association  complements  taught or assessed together
                   related      connected, per adjudication only

    Support columns let consumers pick their own threshold rather than this model
    deciding one:
      courserun_support         distinct course runs with course evidence
      reverse_courserun_support builds_on only: runs where the opposite direction
                                was observed
      course_scope_support      builds_on only: scope_support with each course's
                                reruns and platform copies counted once (its run
                                with the most units), since they have near-identical
                                text; courses are keyed by courseware_current_course_runs
      reverse_scope_support     builds_on only: the same for the opposite direction;
                                direction_consistency is course_scope_support /
                                (course_scope_support + reverse_scope_support)
      co_taught_lift            complements only: observed co-taught unit count
                                over the count expected if the two were taught
                                independently across all taught units
    first_observed_at and last_observed_at show how the edge has evolved; an edge
    whose evidence disappears (an extraction version replaced) drops out on the
    next build.

    builds_on is the one relation this model holds to a support rule, because one
    unit's extraction is one model call and LLMs over-predict reliance. An edge passes
    (passes_support_rule) when course_scope_support reaches
    courseware_prerequisite_min_scope_support and direction_consistency reaches
    courseware_prerequisite_min_direction_consistency, so only the dominant direction
    of a contested pair passes. Every supporting course contributes at least one
    unit, so a course count could never pass an edge this misses. The rule filters
    noise from repeated observations; it does not show that learning from needs to
    (two units can each use to in an exercise unrelated to from), so a passing edge
    is still an observation, not a prerequisite. Edges that fail keep their rows,
    with support_rule_failure. Every other relation passes, and its consumers apply
    their own thresholds to the support columns as before.
#}
with evidence as (
    select * from {{ ref('int__courseware__concept_relation_evidence') }}
)

, course_runs as (
    {{ courseware_current_course_runs() }}
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

-- units per builds_on run, keyed by course as bridge_courserun_concept keys it, so a
-- course's reruns and its copies on other platforms count once. A run that is not a
-- current dim_course_run is keyed by its run id.
, builds_on_course_runs as (
    select
        evidence.from_concept_id
        , evidence.to_concept_id
        , coalesce(
            course_runs.course_key
            , {{ courseware_course_key('evidence.courserun_readable_id') }}
        ) as course_key
        , evidence.platform
        , evidence.courserun_readable_id
        , sum(evidence.scope_count) as scope_count
    from evidence
    left join course_runs
        on
            evidence.platform = course_runs.platform
            and evidence.courserun_readable_id = course_runs.courserun_readable_id
    where evidence.relation = 'builds_on'
    group by
        evidence.from_concept_id
        , evidence.to_concept_id
        , coalesce(
            course_runs.course_key
            , {{ courseware_course_key('evidence.courserun_readable_id') }}
        )
        , evidence.platform
        , evidence.courserun_readable_id
)

, builds_on_course_support as (
    select
        from_concept_id
        , to_concept_id
        , sum(scope_count) as course_scope_support
    from (
        select
            from_concept_id
            , to_concept_id
            , course_key
            , max(scope_count) as scope_count
        from builds_on_course_runs
        group by from_concept_id, to_concept_id, course_key
    ) as per_course
    group by from_concept_id, to_concept_id
)

, builds_on_direction as (
    select
        support.from_concept_id
        , support.to_concept_id
        , support.course_scope_support
        , coalesce(reverse_support.course_scope_support, 0) as reverse_scope_support
        , cast(support.course_scope_support as double)
        / (support.course_scope_support + coalesce(reverse_support.course_scope_support, 0))
        as direction_consistency
        , support.course_scope_support >= {{ var('courseware_prerequisite_min_scope_support') }}
        as has_min_support
    from builds_on_course_support as support
    left join builds_on_course_support as reverse_support
        on
            support.from_concept_id = reverse_support.to_concept_id
            and support.to_concept_id = reverse_support.from_concept_id
)

, builds_on_rule as (
    select
        from_concept_id
        , to_concept_id
        , course_scope_support
        , reverse_scope_support
        , direction_consistency
        , case
            when not has_min_support then 'insufficient_support'
            when direction_consistency < {{ var('courseware_prerequisite_min_direction_consistency') }}
                then 'contested_direction'
        end as support_rule_failure
    from builds_on_direction
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
    , builds_on_rule.course_scope_support
    , builds_on_rule.reverse_scope_support
    , builds_on_rule.direction_consistency
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
    , builds_on_rule.support_rule_failure is null as passes_support_rule
    , builds_on_rule.support_rule_failure
from edges
cross join unit_totals
left join edges as reverse_edges
    on
        edges.relation = 'builds_on'
        and reverse_edges.relation = 'builds_on'
        and edges.from_concept_id = reverse_edges.to_concept_id
        and edges.to_concept_id = reverse_edges.from_concept_id
left join builds_on_rule
    on
        edges.relation = 'builds_on'
        and edges.from_concept_id = builds_on_rule.from_concept_id
        and edges.to_concept_id = builds_on_rule.to_concept_id
left join concept_unit_counts as from_units
    on edges.from_concept_id = from_units.concept_id
left join concept_unit_counts as to_units
    on edges.to_concept_id = to_units.concept_id
