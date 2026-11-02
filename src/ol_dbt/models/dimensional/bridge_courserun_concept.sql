{#
    How each course run relates to each concept: teaches, reviews, uses, mentions,
    assesses or requires, from current verified claims.

    requires is declared (a stated prerequisite) or inferred. An inferred requirement
    is an observation about this run: a concept it relies on (assumed knowledge, or a
    concept it uses without explaining) before, or without, any scope of the run
    teaching or reviewing it. It is not a reviewed prerequisite, and nothing here
    says how much of the concept a learner needs. The course's documents (the
    materials scope) accompany its lectures, so a concept they teach counts as taught
    from the start; the context scope teaches nothing.

    An inferred requirement passes the support rule (passes_support_rule) only with
    support beyond one model call, because LLMs over-predict prerequisites and every
    spurious one then matches some course that teaches it. Passing filters that
    noise; it does not accept the requirement:
      - prior_scope_count, the units relying on the concept before the run teaches it,
        is at least courseware_prerequisite_min_scope_support, or course_support,
        the courses with a run that declares the concept or infers it with a
        direction_consistency over the threshold, is at least
        courseware_prerequisite_min_course_support. Courses, not runs, because two
        runs of one course have near-identical text and would corroborate themselves.
        A course is keyed as bridge_courserun_prerequisite_coverage keys it
        (course_key from courseware_current_course_runs, which leaves out the
        platform), so an edxorg and a mitxonline copy of one course count once; and
      - direction_consistency, prior_scope_count over that plus taught_scope_count
        (the scopes that teach it later, which says the run covers it itself), is at
        least courseware_prerequisite_min_direction_consistency.
    Candidates that fail keep their rows, with support_rule_failure, for review.
    Declared requirements and the other relations always pass.

    tool_status describes an inferred requirement of a tool from this run's own claims
    only. A concept is a tool here when most of the run's claims that give it a kind
    say tool (a tie counts as tool), not by dim_concept.concept_kind, which votes
    across every course. Assumed-knowledge and declared-prerequisite claims give no
    kind, so a run that names the concept only through those takes
    dim_concept.concept_kind; the status still comes from the run's claims alone:
      assumed     the run says learners should already know it: an assumed_knowledge
                  claim in any scope, or a declared prerequisite or recommended
                  background
      introduced  otherwise, the run teaches it in some scope (after first using it)
      unresolved  the run only uses it, which can mean it demonstrates the tool as it
                  goes or that it expects learners to know it; the claims don't say
    Whether another course teaches the tool is left to
    bridge_courserun_prerequisite_coverage, where it can rank possible preparation.
    It never changes this status, so adding an unrelated course leaves an unchanged
    run's tool status as it was. tool_status does not change the support rule.

    Every relation is kept here for review whatever its claims' review status;
    accepted_claim_count is how many of claim_count are accepted (for an inferred
    requirement, only those made before the run teaches the concept), and
    learner-facing views take only relations with at least one.
#}
-- one row per (claim, concept): an assessment that lists two labels of one concept
-- is one claim behind the relation, not two
with mentions as (
    select distinct
        platform
        , courserun_readable_id
        , scope_id
        , scope_kind
        , scope_position
        , extraction_key
        , claim_ordinal
        , claim_kind
        , teaching_status
        , concept_kind
        , concept_id
        , prerequisite_kind
        , review_status
    from {{ ref('int__courseware__resolved_concept_mentions') }}
)

, course_runs as (
    {{ courseware_current_course_runs() }}
)

, concepts as (
    select * from {{ ref('dim_concept') }}
)

-- whether the run's own claims call each concept a tool; assumed-knowledge and
-- declared-prerequisite claims carry no kind, so a concept only those mention has
-- no row and is judged by dim_concept.concept_kind
, run_concept_kinds as (
    select
        platform
        , courserun_readable_id
        , concept_id
        , sum(case when concept_kind = 'tool' then 1 else 0 end)
        >= sum(case when concept_kind != 'tool' then 1 else 0 end) as is_tool
    from mentions
    where concept_kind is not null
    group by platform, courserun_readable_id, concept_id
)

-- the course each run belongs to, so a course's reruns and its copies on other
-- platforms count once. A run that is not a current dim_course_run is keyed by its
-- run id, as courseware_current_course_runs keys a run without a course_fk.
, run_courses as (
    select distinct
        mentions.platform
        , mentions.courserun_readable_id
        , coalesce(
            course_runs.course_key
            , {{ courseware_course_key('mentions.courserun_readable_id') }}
        ) as course_key
    from mentions
    left join course_runs
        on
            mentions.platform = course_runs.platform
            and mentions.courserun_readable_id = course_runs.courserun_readable_id
)

, direct_relations as (
    select
        platform
        , courserun_readable_id
        , concept_id
        , case
            when claim_kind = 'concept' and teaching_status = 'taught' then 'teaches'
            when claim_kind = 'concept' and teaching_status = 'reviewed' then 'reviews'
            when claim_kind = 'concept' and teaching_status = 'used' then 'uses'
            when claim_kind = 'concept' and teaching_status = 'mentioned' then 'mentions'
            when claim_kind = 'assessment_expectation' then 'assesses'
            -- a recommendation or a corequisite is not something to have before the run
            when claim_kind = 'declared_prerequisite' and prerequisite_kind = 'prerequisite' then 'requires'
            when claim_kind = 'declared_prerequisite' and prerequisite_kind = 'corequisite' then 'requires_concurrently'
            when claim_kind = 'declared_prerequisite' and prerequisite_kind = 'recommended' then 'recommends'
        end as relation
        , case when claim_kind = 'declared_prerequisite' then 'declared' end as requirement_source
        , scope_position
        , scope_id
        , review_status
    from mentions
    where claim_kind in ('concept', 'assessment_expectation', 'declared_prerequisite')
)

, first_taught as (
    select
        platform
        , courserun_readable_id
        , concept_id
        , min(case when scope_kind = 'materials' then 0 else scope_position end) as first_taught_position
        , count(distinct case when teaching_status = 'taught' then scope_id end) as taught_scope_count
    from mentions
    where
        claim_kind = 'concept'
        and teaching_status in ('taught', 'reviewed')
        and scope_kind != 'context'
    group by platform, courserun_readable_id, concept_id
)

-- each unit that relies on the concept, with how many of its claims do
, relied_on as (
    select
        platform
        , courserun_readable_id
        , concept_id
        , scope_id
        , min(scope_position) as relied_position
        , count(*) as claim_count
        , count(case when review_status = 'accepted' then 1 end) as accepted_claim_count
    from mentions
    where
        scope_kind = 'unit'
        -- a dataset a course works with is material, not preparation
        and coalesce(concept_kind, '') != 'dataset'
        and (
            claim_kind = 'assumed_knowledge'
            or (claim_kind = 'concept' and teaching_status = 'used')
        )
    group by platform, courserun_readable_id, concept_id, scope_id
)

, inferred_candidates as (
    select
        relied_on.platform
        , relied_on.courserun_readable_id
        , relied_on.concept_id
        , min(relied_on.relied_position) as first_scope_position
        , sum(relied_on.claim_count) as claim_count
        -- only an accepted claim that relies on the concept before any scope teaches
        -- it supports the requirement; a later one is use of what the run taught
        , sum(
            case
                when
                    first_taught.first_taught_position is null
                    or relied_on.relied_position < first_taught.first_taught_position
                    then relied_on.accepted_claim_count
                else 0
            end
        ) as accepted_claim_count
        , count(*) as scope_count
        , array_agg(relied_on.scope_id order by relied_on.relied_position, relied_on.scope_id) as relied_scope_ids
        , sum(
            case
                when
                    first_taught.first_taught_position is null
                    or relied_on.relied_position < first_taught.first_taught_position
                    then 1
                else 0
            end
        ) as prior_scope_count
        , coalesce(max(first_taught.taught_scope_count), 0) as taught_scope_count
    from relied_on
    left join first_taught
        on
            relied_on.platform = first_taught.platform
            and relied_on.courserun_readable_id = first_taught.courserun_readable_id
            and relied_on.concept_id = first_taught.concept_id
    group by relied_on.platform, relied_on.courserun_readable_id, relied_on.concept_id
    having
        max(first_taught.first_taught_position) is null
        or max(first_taught.first_taught_position) > min(relied_on.relied_position)
)

-- concepts a run says learners should already know, in any scope
, expected_by_run as (
    select distinct
        platform
        , courserun_readable_id
        , concept_id
    from mentions
    where
        claim_kind = 'assumed_knowledge'
        or (claim_kind = 'declared_prerequisite' and prerequisite_kind in ('prerequisite', 'recommended'))
)

, inferred_directions as (
    select
        inferred_candidates.*
        , cast(inferred_candidates.prior_scope_count as double)
        / (inferred_candidates.prior_scope_count + inferred_candidates.taught_scope_count) as direction_consistency
        , case
            when not coalesce(run_concept_kinds.is_tool, concepts.concept_kind = 'tool', false) then cast(null as varchar)
            when expected_by_run.concept_id is not null then 'assumed'
            when inferred_candidates.taught_scope_count > 0 then 'introduced'
            else 'unresolved'
        end as tool_status
    from inferred_candidates
    left join run_concept_kinds
        on
            inferred_candidates.platform = run_concept_kinds.platform
            and inferred_candidates.courserun_readable_id = run_concept_kinds.courserun_readable_id
            and inferred_candidates.concept_id = run_concept_kinds.concept_id
    left join concepts
        on inferred_candidates.concept_id = concepts.concept_pk
    left join expected_by_run
        on
            inferred_candidates.platform = expected_by_run.platform
            and inferred_candidates.courserun_readable_id = expected_by_run.courserun_readable_id
            and inferred_candidates.concept_id = expected_by_run.concept_id
)

-- courses that require each concept, for cross-course support: declared, or inferred
-- by a run whose own evidence points the right way, keyed by run_courses
, requiring_courses as (
    select
        requiring.concept_id
        , count(distinct run_courses.course_key) as course_support
    from (
        select
            platform
            , courserun_readable_id
            , concept_id
        from inferred_directions
        where direction_consistency >= {{ var('courseware_prerequisite_min_direction_consistency') }}
        union all
        select
            platform
            , courserun_readable_id
            , concept_id
        from direct_relations
        where relation = 'requires'
    ) as requiring
    inner join run_courses
        on
            requiring.platform = run_courses.platform
            and requiring.courserun_readable_id = run_courses.courserun_readable_id
    group by requiring.concept_id
)

, inferred_requirements as (
    select
        inferred_directions.platform
        , inferred_directions.courserun_readable_id
        , inferred_directions.concept_id
        , 'requires' as relation
        , 'inferred' as requirement_source
        , inferred_directions.first_scope_position
        , inferred_directions.claim_count
        , inferred_directions.accepted_claim_count
        , inferred_directions.scope_count
        , inferred_directions.relied_scope_ids
        , inferred_directions.prior_scope_count
        , inferred_directions.taught_scope_count
        , coalesce(requiring_courses.course_support, 0) as course_support
        , inferred_directions.direction_consistency
        , inferred_directions.tool_status
    from inferred_directions
    left join requiring_courses
        on inferred_directions.concept_id = requiring_courses.concept_id
)

, relations as (
    select
        platform
        , courserun_readable_id
        , concept_id
        , relation
        , requirement_source
        , min(scope_position) as first_scope_position
        , count(*) as claim_count
        , count(case when review_status = 'accepted' then 1 end) as accepted_claim_count
        , count(distinct scope_id) as scope_count
        , {{ null_varchar_array() }} as relied_scope_ids
        , cast(null as bigint) as prior_scope_count
        , cast(null as bigint) as taught_scope_count
        , cast(null as bigint) as course_support
        , cast(null as double) as direction_consistency
        , cast(null as varchar) as tool_status
    from direct_relations
    group by platform, courserun_readable_id, concept_id, relation, requirement_source

    union all

    select * from inferred_requirements
)

, judged as (
    select
        *
        , case
            when
                requirement_source = 'inferred'
                and prior_scope_count < {{ var('courseware_prerequisite_min_scope_support') }}
                and course_support < {{ var('courseware_prerequisite_min_course_support') }}
                then 'insufficient_support'
            when
                requirement_source = 'inferred'
                and direction_consistency < {{ var('courseware_prerequisite_min_direction_consistency') }}
                then 'contested_direction'
        end as support_rule_failure
    from relations
)

select
    {{ dbt_utils.generate_surrogate_key([
        'judged.platform', 'judged.courserun_readable_id',
        'judged.concept_id', 'judged.relation', 'judged.requirement_source'
    ]) }} as courserun_concept_pk
    , judged.platform
    , judged.courserun_readable_id
    , course_runs.courserun_pk as courserun_fk
    , judged.concept_id as concept_fk
    , judged.relation
    , judged.requirement_source
    , judged.first_scope_position
    , judged.claim_count
    , judged.accepted_claim_count
    , judged.scope_count
    , judged.relied_scope_ids
    , judged.prior_scope_count
    , judged.taught_scope_count
    , judged.course_support
    , judged.direction_consistency
    , judged.tool_status
    , judged.support_rule_failure is null as passes_support_rule
    , judged.support_rule_failure
from judged
left join course_runs
    on
        judged.platform = course_runs.platform
        and judged.courserun_readable_id = course_runs.courserun_readable_id
