{#
    How each course run relates to each concept: teaches, reviews, uses, mentions,
    assesses or requires, from current verified claims.

    requires is declared (a stated prerequisite) or inferred. An inferred requirement
    is a concept the run relies on (assumed knowledge, or a concept it uses without
    explaining) before, or without, any scope of the run teaching or reviewing it.
    The course's documents (the materials scope) accompany its lectures, so a concept
    they teach counts as taught from the start; the context scope teaches nothing.

    An inferred requirement is published (is_published) only with support beyond one
    model call, because LLMs over-predict prerequisites and every spurious one then
    matches some course that teaches it:
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

    A tool the run introduces is not a requirement either (unpublished_reason
    introduced_by_run): a concept whose dim_concept.concept_kind is tool, that the run
    only uses (no claim of the run, in any scope, assumes it or declares it a
    prerequisite or recommended background), and that no other course teaches or
    reviews. Courses demonstrate libraries such as scikit-learn and Seaborn as they
    go, so requiring them would leave a prerequisite no course could ever satisfy.
    "Other course" and "teaches or reviews" are as bridge_courserun_prerequisite_coverage
    reads them (the same course_key, a current run, and any teaches or reviews row),
    so a tool is introduced only where coverage would find no direct provider. Where
    the tool first appears does not separate these cases: on the pilot, the
    introduced tools first appear between the 4th and 10th unit, like pandas, which
    an earlier course teaches and which stays a requirement.

    Unpublished candidates keep their rows, with unpublished_reason, for review.
    Declared requirements and the other relations are always published.
#}
with mentions as (
    select
        platform
        , courserun_readable_id
        , scope_id
        , scope_kind
        , scope_position
        , claim_kind
        , teaching_status
        , concept_kind
        , concept_id
        , prerequisite_kind
    from {{ ref('int__courseware__resolved_concept_mentions') }}
)

, course_runs as (
    {{ courseware_current_course_runs() }}
)

, concepts as (
    select * from {{ ref('dim_concept') }}
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
    from mentions
    where
        claim_kind in ('concept', 'assessment_expectation', 'declared_prerequisite')
        -- "no prerequisites": its label names what is not required
        and coalesce(prerequisite_kind, '') != 'explicitly_none'
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

, relied_on as (
    select
        platform
        , courserun_readable_id
        , concept_id
        , scope_id
        , min(scope_position) as relied_position
        , count(*) as claim_count
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
        , count(*) as scope_count
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

-- candidates some other course teaches or reviews, as coverage would find them:
-- coverage's providers are current runs only
, taught_elsewhere as (
    select distinct
        inferred_candidates.platform
        , inferred_candidates.courserun_readable_id
        , inferred_candidates.concept_id
    from inferred_candidates
    inner join run_courses
        on
            inferred_candidates.platform = run_courses.platform
            and inferred_candidates.courserun_readable_id = run_courses.courserun_readable_id
    inner join direct_relations as teaching
        on
            inferred_candidates.concept_id = teaching.concept_id
            and teaching.relation in ('teaches', 'reviews')
    inner join course_runs as teaching_courses
        on
            teaching.platform = teaching_courses.platform
            and teaching.courserun_readable_id = teaching_courses.courserun_readable_id
    where teaching_courses.course_key != run_courses.course_key
)

, inferred_directions as (
    select
        inferred_candidates.*
        , cast(inferred_candidates.prior_scope_count as double)
        / (inferred_candidates.prior_scope_count + inferred_candidates.taught_scope_count) as direction_consistency
        , coalesce(
            concepts.concept_kind = 'tool'
            and expected_by_run.concept_id is null
            and taught_elsewhere.concept_id is null
            , false
        ) as is_introduced_by_run
    from inferred_candidates
    left join concepts
        on inferred_candidates.concept_id = concepts.concept_pk
    left join expected_by_run
        on
            inferred_candidates.platform = expected_by_run.platform
            and inferred_candidates.courserun_readable_id = expected_by_run.courserun_readable_id
            and inferred_candidates.concept_id = expected_by_run.concept_id
    left join taught_elsewhere
        on
            inferred_candidates.platform = taught_elsewhere.platform
            and inferred_candidates.courserun_readable_id = taught_elsewhere.courserun_readable_id
            and inferred_candidates.concept_id = taught_elsewhere.concept_id
)

-- courses that require each concept, for cross-course support: declared, or inferred
-- by a run whose own evidence points the right way and that does not introduce it
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
        where
            direction_consistency >= {{ var('courseware_prerequisite_min_direction_consistency') }}
            and not is_introduced_by_run
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
        , inferred_directions.scope_count
        , inferred_directions.prior_scope_count
        , inferred_directions.taught_scope_count
        , coalesce(requiring_courses.course_support, 0) as course_support
        , inferred_directions.direction_consistency
        , inferred_directions.is_introduced_by_run
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
        , count(distinct scope_id) as scope_count
        , cast(null as bigint) as prior_scope_count
        , cast(null as bigint) as taught_scope_count
        , cast(null as bigint) as course_support
        , cast(null as double) as direction_consistency
        , false as is_introduced_by_run
    from direct_relations
    group by platform, courserun_readable_id, concept_id, relation, requirement_source

    union all

    select * from inferred_requirements
)

, judged as (
    select
        *
        , case
            when is_introduced_by_run then 'introduced_by_run'
            when
                requirement_source = 'inferred'
                and prior_scope_count < {{ var('courseware_prerequisite_min_scope_support') }}
                and course_support < {{ var('courseware_prerequisite_min_course_support') }}
                then 'insufficient_support'
            when
                requirement_source = 'inferred'
                and direction_consistency < {{ var('courseware_prerequisite_min_direction_consistency') }}
                then 'contested_direction'
        end as unpublished_reason
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
    , judged.scope_count
    , judged.prior_scope_count
    , judged.taught_scope_count
    , judged.course_support
    , judged.direction_consistency
    , judged.unpublished_reason is null as is_published
    , judged.unpublished_reason
from judged
left join course_runs
    on
        judged.platform = course_runs.platform
        and judged.courserun_readable_id = course_runs.courserun_readable_id
