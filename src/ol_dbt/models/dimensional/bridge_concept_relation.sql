{#
    Concept-to-concept edges with the evidence behind them, one row per
    (from, to, relation). This is the graph's edge table and the input to
    ml.lib.courseware.graph.

    Every row is a candidate relation aggregated from observations
    (int__courseware__concept_relation_evidence, which keeps the course, unit,
    source and extractor version of each), not an accepted assertion: review_status
    stays proposed until a review workflow exists.

    relation_family groups relations by what they mean for a learner:
      hierarchy    broader      from is a kind or part of to; the resolver's verdicts
                                do not say which, so the two are not separated yet
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
      course_scope_support      course relations only: scope_support with each
                                course's reruns and platform copies counted once
                                (its run with the most units), since they have
                                near-identical text; courses are keyed by
                                courseware_current_course_runs
      reverse_scope_support     builds_on only: the same for the opposite direction;
                                direction_consistency is course_scope_support /
                                (course_scope_support + reverse_scope_support)
      co_taught_lift            complements only: observed co-taught unit count
                                over the count expected if the two were taught
                                independently across all taught units
      verdict_support, contrary_verdict_support
                                adjudicated relations only: resolution verdicts
                                agreeing with and against the relation
      cross_signal_support      evidence for the pair from the other source: for
                                an adjudicated relation, course evidence (units
                                co-teaching, co-assessing or sequencing the pair,
                                either direction, summed within a run, then each
                                course's largest run); for a
                                course relation,
                                verdicts saying the pair is connected
    first_observed_at and last_observed_at show how the edge has evolved; an edge
    whose evidence disappears (an extraction version replaced) drops out on the
    next build.

    support_score combines those signals in [0, 1]. It is an uncalibrated ranking
    heuristic, not a probability that the relation holds: it treats each observation
    as an independent, noisy vote (noisy-OR), scaled by the share of same-source
    evidence that agrees, but units of one course share source material and
    authors, and adjudication and extraction are related model judgements, so the
    votes are not independent. Cross-signal evidence says the pair is connected, not
    that this relation or direction holds. The raw components above stay beside it
    for calibration against independently annotated relations.
      support_score = consistency
        * (1 - (1 - own_reliability) ^ own_support
             * (1 - cross_signal_reliability) ^ cross_signal_support)
    own_support is verdict_support for adjudicated relations (reliability 0.5 each,
    a direct pairwise judgement) and course_scope_support for course relations (0.3
    each, one extraction call implying the edge; a rerun of a course adds nothing,
    as for the support rule). Cross-signal evidence counts 0.2 each. consistency
    is direction_consistency for builds_on, verdict_support / (verdict_support +
    contrary_verdict_support) for adjudicated relations, and 1 otherwise. Adding
    agreeing evidence never lowers the score and adding contrary evidence never
    raises it. The reliabilities are priors, not fitted values.
    verdict_support is at least 1: the relation is in the resolver's snapshot
    because a verdict, or a carried edge's vote, proposed it, even when none of the
    labels that voted is still assigned, so the proposal counts as one verdict. The
    score is rounded to 4 places, so the snapshot does not version an edge over a
    last-bit difference between engines or plans.

    Support rules (passes_support_rule, with support_rule_failure on the rest, which
    keep their rows). They filter noise; they do not accept a relation. Every
    relation other than builds_on and broader passes, and its consumers apply their
    own thresholds to the support columns.
    - builds_on passes when course_scope_support reaches
      courseware_prerequisite_min_scope_support and direction_consistency reaches
      courseware_prerequisite_min_direction_consistency, because one unit's
      extraction is one model call and LLMs over-predict reliance
      (insufficient_support, contested_direction). Every supporting course
      contributes at least one unit, so a course count could never pass an edge
      this misses. Cross-signal agreement raises its score but never passes it: a
      broader verdict says the two concepts are a kind or part of one another,
      which runs either way in learning order.
    - broader is held to a strict partial order, since a kind-of or part-of cycle
      cannot all be true. Broader edges are ranked by adjudication score
      (support_score without the cross-signal term), then by how many broader edges
      point at the broader concept (an edge into a concept many others sit under is
      the likelier one), then by concept ids, so the order is total. This ranking
      is a heuristic for which edge of a contradiction to send to review. The
      ranking reads only the adjudicated table, so the walk does not recompute the
      whole pipeline once per step on engines that inline CTEs. An edge whose
      broader concept leads back to its narrower one through edges ranked above it
      is the lowest-ranked edge of that cycle and fails (hierarchy_cycle). Every
      cycle loses its lowest-ranked edge, in one pass over all broader edges, so
      where cycles overlap an edge can be cut for a cycle a cut above it already
      broke (z < x for z < x < y < z, when x < y is cut for x < y < w < x).
      assert_bridge_concept_relation_hierarchy_cycle_overprune counts the cut
      edges a second pass over the survivors would keep, an upper bound on that.
      The walk is bounded at max_cycle_length edges and fails closed: an edge
      whose walk could go on past the bound without having closed a cycle fails
      too (hierarchy_cycle_unresolved), so the passing broader edges are acyclic
      whatever the length of a cycle, and the acyclic test on them, bounded the
      same way, holds.
    - broader and builds_on are judged independently. A pair can be both (a
      private method is a kind of method and builds on understanding methods), so
      neither predicate's score is used to fail the other.
#}
{%- set verdict_reliability = courseware_verdict_reliability() -%}
{%- set unit_reliability = 0.3 -%}
{%- set cross_signal_reliability = 0.2 -%}
{#- the acyclic test's default bound -#}
{%- set max_cycle_length = 32 -%}
with evidence as (
    select * from {{ ref('int__courseware__concept_relation_evidence') }}
)

, course_runs as (
    {{ courseware_current_course_runs() }}
)

, adjudicated as (
    select * from {{ ref('int__courseware__adjudicated_concept_relations') }}
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

-- units per course relation and run, over every evidence source, keyed by course as
-- bridge_courserun_concept keys it, so a course's reruns and its copies on other
-- platforms count once (its run with the most units). A run that is not a current
-- dim_course_run is keyed by courseware_course_key of its run id.
, course_relation_runs as (
    select
        evidence.from_concept_id
        , evidence.to_concept_id
        , evidence.relation
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
    where evidence.evidence_source != 'adjudication'
    group by
        evidence.from_concept_id
        , evidence.to_concept_id
        , evidence.relation
        , coalesce(
            course_runs.course_key
            , {{ courseware_course_key('evidence.courserun_readable_id') }}
        )
        , evidence.platform
        , evidence.courserun_readable_id
)

, course_support as (
    select
        from_concept_id
        , to_concept_id
        , relation
        , sum(scope_count) as course_scope_support
    from (
        select
            from_concept_id
            , to_concept_id
            , relation
            , course_key
            , max(scope_count) as scope_count
        from course_relation_runs
        group by from_concept_id, to_concept_id, relation, course_key
    ) as per_course
    group by from_concept_id, to_concept_id, relation
)

, builds_on_course_support as (
    select
        from_concept_id
        , to_concept_id
        , course_scope_support
    from course_support
    where relation = 'builds_on'
)

-- course units for the unordered pair, over every relation and direction: summed
-- within a run first, so a course whose reruns split the pair's evidence across
-- directions or relations still counts its one largest run
, pair_course_support as (
    select
        low_concept_id
        , high_concept_id
        , sum(scope_count) as course_unit_count
    from (
        select
            low_concept_id
            , high_concept_id
            , course_key
            , max(scope_count) as scope_count
        from (
            select
                least(from_concept_id, to_concept_id) as low_concept_id
                , greatest(from_concept_id, to_concept_id) as high_concept_id
                , course_key
                , platform
                , courserun_readable_id
                , sum(scope_count) as scope_count
            from course_relation_runs
            group by
                least(from_concept_id, to_concept_id)
                , greatest(from_concept_id, to_concept_id)
                , course_key
                , platform
                , courserun_readable_id
        ) as per_run
        group by low_concept_id, high_concept_id, course_key
    ) as per_course
    group by low_concept_id, high_concept_id
)

, pair_verdict_support as (
    select
        least(from_concept_id, to_concept_id) as low_concept_id
        , greatest(from_concept_id, to_concept_id) as high_concept_id
        , max(pair_verdict_count) as connected_verdict_count
    from adjudicated
    group by least(from_concept_id, to_concept_id), greatest(from_concept_id, to_concept_id)
)

-- the adjudication terms of support_score: the share of verdicts that agree,
-- and the chance that every agreeing verdict is wrong; the proposal itself is the
-- floor of one supporting verdict
, adjudication_scores as (
    select
        from_concept_id
        , to_concept_id
        , relation
        , verdict_support
        , contrary_verdict_support
        , cast(verdict_support as double) / (verdict_support + contrary_verdict_support) as verdict_consistency
        , power(1.0 - {{ verdict_reliability }}, verdict_support) as verdict_miss
    from (
        select
            from_concept_id
            , to_concept_id
            , relation
            , greatest(supporting_verdict_count, 1) as verdict_support
            , contrary_verdict_count as contrary_verdict_support
        from adjudicated
    ) as adjudicated_support
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

, builds_on_support as (
    select
        from_concept_id
        , to_concept_id
        , reverse_scope_support
        , direction_consistency
        , case
            when not has_min_support then 'insufficient_support'
            when direction_consistency < {{ var('courseware_prerequisite_min_direction_consistency') }}
                then 'contested_direction'
        end as support_rule_failure
    from builds_on_direction
)

, signals as (
    select
        edges.*
        , course_support.course_scope_support
        , builds_on_support.reverse_scope_support
        , builds_on_support.direction_consistency
        , builds_on_support.support_rule_failure as builds_on_rule_failure
        , adjudication_scores.verdict_support
        , adjudication_scores.contrary_verdict_support
        , adjudication_scores.verdict_consistency
        , adjudication_scores.verdict_miss
        , case
            when edges.is_adjudicated then coalesce(pair_course_support.course_unit_count, 0)
            else coalesce(pair_verdict_support.connected_verdict_count, 0)
        end as cross_signal_support
    from edges
    left join builds_on_support
        on
            edges.relation = 'builds_on'
            and edges.from_concept_id = builds_on_support.from_concept_id
            and edges.to_concept_id = builds_on_support.to_concept_id
    left join course_support
        on
            edges.from_concept_id = course_support.from_concept_id
            and edges.to_concept_id = course_support.to_concept_id
            and edges.relation = course_support.relation
    left join adjudication_scores
        on
            edges.from_concept_id = adjudication_scores.from_concept_id
            and edges.to_concept_id = adjudication_scores.to_concept_id
            and edges.relation = adjudication_scores.relation
    left join pair_course_support
        on
            least(edges.from_concept_id, edges.to_concept_id) = pair_course_support.low_concept_id
            and greatest(edges.from_concept_id, edges.to_concept_id) = pair_course_support.high_concept_id
    left join pair_verdict_support
        on
            least(edges.from_concept_id, edges.to_concept_id) = pair_verdict_support.low_concept_id
            and greatest(edges.from_concept_id, edges.to_concept_id) = pair_verdict_support.high_concept_id
)

, scored as (
    select
        signals.*
        , round(
            case
                when signals.is_adjudicated then signals.verdict_consistency
                else coalesce(signals.direction_consistency, 1.0)
            end
            * (
                1.0
                - case
                    when signals.is_adjudicated then signals.verdict_miss
                    else power(1.0 - {{ unit_reliability }}, signals.course_scope_support)
                end
                * power(1.0 - {{ cross_signal_reliability }}, signals.cross_signal_support)
            )
            , 4
        ) as support_score
    from signals
)

-- broader edges in cycle-breaking order, lowest first
, hierarchy_edges as (
    {{ courseware_hierarchy_edges("(select * from adjudication_scores where relation = 'broader')") }}
)

{{ courseware_hierarchy_cycle_cuts('hierarchy_cycle_edges', 'hierarchy_edges', 'hierarchy_edges', max_cycle_length) }}

, support_rules as (
    select
        scored.from_concept_id
        , scored.to_concept_id
        , scored.relation
        , coalesce(
            scored.builds_on_rule_failure
            , case hierarchy_cycle_edges.cut_reason
                when 'cycle' then 'hierarchy_cycle'
                when 'unresolved' then 'hierarchy_cycle_unresolved'
            end
        ) as support_rule_failure
    from scored
    left join hierarchy_cycle_edges
        on
            scored.relation = 'broader'
            and scored.from_concept_id = hierarchy_cycle_edges.from_concept_id
            and scored.to_concept_id = hierarchy_cycle_edges.to_concept_id
)

select
    {{ dbt_utils.generate_surrogate_key(['scored.from_concept_id', 'scored.to_concept_id', 'scored.relation']) }}
    as concept_relation_pk
    , scored.from_concept_id as from_concept_fk
    , scored.to_concept_id as to_concept_fk
    , scored.relation
    , case scored.relation
        when 'broader' then 'hierarchy'
        when 'builds_on' then 'sequence'
        when 'practiced_with' then 'practice'
        when 'close' then 'equivalence'
        else 'association'
    end as relation_family
    , scored.courserun_support
    , scored.scope_support
    , coalesce(reverse_edges.courserun_support, 0) as reverse_courserun_support
    , scored.course_scope_support
    , scored.reverse_scope_support
    , scored.direction_consistency
    , case
        when scored.relation = 'complements' and scored.co_taught_unit_count > 0
            then scored.co_taught_unit_count * cast(unit_totals.taught_unit_count as double)
            / (from_units.taught_unit_count * to_units.taught_unit_count)
    end as co_taught_lift
    , scored.verdict_support
    , scored.contrary_verdict_support
    , scored.cross_signal_support
    , scored.support_score
    , scored.is_adjudicated
    , scored.has_sequence_evidence
    , scored.has_co_taught_evidence
    , scored.has_co_assessed_evidence
    , scored.first_observed_at
    , scored.last_observed_at
    , 'proposed' as review_status
    , support_rules.support_rule_failure is null as passes_support_rule
    , support_rules.support_rule_failure
from scored
cross join unit_totals
inner join support_rules
    on
        scored.from_concept_id = support_rules.from_concept_id
        and scored.to_concept_id = support_rules.to_concept_id
        and scored.relation = support_rules.relation
left join edges as reverse_edges
    on
        scored.relation = 'builds_on'
        and reverse_edges.relation = 'builds_on'
        and scored.from_concept_id = reverse_edges.to_concept_id
        and scored.to_concept_id = reverse_edges.from_concept_id
left join concept_unit_counts as from_units
    on scored.from_concept_id = from_units.concept_id
left join concept_unit_counts as to_units
    on scored.to_concept_id = to_units.concept_id
