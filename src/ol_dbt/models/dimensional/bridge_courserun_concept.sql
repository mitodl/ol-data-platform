{#
    How each course run relates to each concept: teaches, reviews, uses, mentions,
    assesses or requires, from current verified claims.

    requires is declared (a stated prerequisite) or inferred. An inferred requirement
    is a concept the run relies on (assumed knowledge, or a concept it uses without
    explaining) before, or without, any scope of the run teaching or reviewing it.
    The course's documents (the materials scope) accompany its lectures, so a concept
    they teach counts as taught from the start; the context scope teaches nothing.

    Every relation is kept here for review whatever its claims' review status;
    accepted_claim_count is how many of claim_count are accepted, and learner-facing
    views take only relations with at least one.
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
        , review_status
    from {{ ref('int__courseware__resolved_concept_mentions') }}
)

, course_runs as (
    select * from {{ ref('dim_course_run') }}
    where is_current
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
        , min(scope_position) as first_relied_position
        , count(*) as claim_count
        , count(case when review_status = 'accepted' then 1 end) as accepted_claim_count
        , count(distinct scope_id) as scope_count
    from mentions
    where
        scope_kind = 'unit'
        -- a dataset a course works with is material, not preparation
        and coalesce(concept_kind, '') != 'dataset'
        and (
            claim_kind = 'assumed_knowledge'
            or (claim_kind = 'concept' and teaching_status = 'used')
        )
    group by platform, courserun_readable_id, concept_id
)

, inferred_requirements as (
    select
        relied_on.platform
        , relied_on.courserun_readable_id
        , relied_on.concept_id
        , 'requires' as relation
        , 'inferred' as requirement_source
        , relied_on.first_relied_position as first_scope_position
        , relied_on.claim_count
        , relied_on.accepted_claim_count
        , relied_on.scope_count
    from relied_on
    left join first_taught
        on
            relied_on.platform = first_taught.platform
            and relied_on.courserun_readable_id = first_taught.courserun_readable_id
            and relied_on.concept_id = first_taught.concept_id
    where
        first_taught.first_taught_position is null
        or first_taught.first_taught_position > relied_on.first_relied_position
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
    from direct_relations
    group by platform, courserun_readable_id, concept_id, relation, requirement_source

    union all

    select * from inferred_requirements
)

select
    {{ dbt_utils.generate_surrogate_key([
        'relations.platform', 'relations.courserun_readable_id',
        'relations.concept_id', 'relations.relation', 'relations.requirement_source'
    ]) }} as courserun_concept_pk
    , relations.platform
    , relations.courserun_readable_id
    , course_runs.courserun_pk as courserun_fk
    , relations.concept_id as concept_fk
    , relations.relation
    , relations.requirement_source
    , relations.first_scope_position
    , relations.claim_count
    , relations.accepted_claim_count
    , relations.scope_count
from relations
left join course_runs
    on
        relations.platform = course_runs.platform
        and relations.courserun_readable_id = course_runs.courserun_readable_id
