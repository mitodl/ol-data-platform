{#
    What each Learn course teaches, assesses and expects, as the next knowledge
    release would publish it, from the course's run in int__courseware__learn_course_runs
    (its most recently extracted source-eligible run), keyed as Learn keys the course
    (etl_source, readable_id). The courseware_knowledge_release asset copies these
    rows into a release, and integrations__learn__resource_knowledge reads them from
    the published one.

    The concept lists name only concepts int__courseware__release_concepts would
    publish as learner-facing, by the same label, and only relations an accepted
    claim supports (an inferred requirement must also pass the support rule), the
    same rows int__courseware__release_resource_concepts publishes by id. They render
    context_document and are not published as columns. The statement lists take only
    accepted claims. Accepting a concept does not accept the claims naming it.

    context_document renders the same arrays as markdown, deterministically, so it can
    be embedded as a resource's search text (the embedding substrate's
    resource_knowledge_markdown_v1 builder) without another model call.
#}
{% set concept_order = 'run_bridge.claim_count desc, concepts.name ASC' %}

with learn_runs as (
    select * from {{ ref('int__courseware__learn_course_runs') }}
)

, run_bridge as (
    select * from {{ ref('bridge_courserun_concept') }}
    -- accepted claims only, and for inferred requires the support rule as well
    where accepted_claim_count > 0 and passes_support_rule
)

, concepts as (
    select * from {{ ref('int__courseware__release_concepts') }}
    where is_learner_facing
)

, claims as (
    select * from {{ ref('tfact_courseware_claim') }}
    where is_current and is_verified and review_status = 'accepted'
)

, concept_lists as (
    select
        run_bridge.courserun_fk
        , {{ array_agg_filter('concepts.name', "run_bridge.relation = 'teaches'", order_by=concept_order) }} as concepts_taught
        , {{ array_agg_filter('concepts.name', "run_bridge.relation = 'reviews'", order_by=concept_order) }} as concepts_reviewed
        , {{ array_agg_filter('concepts.name', "run_bridge.relation = 'uses'", order_by=concept_order) }} as concepts_used
        , {{ array_agg_filter('concepts.name', "run_bridge.relation = 'assesses'", order_by=concept_order) }} as concepts_assessed
        , {{ array_agg_filter(
            'concepts.name'
            , "run_bridge.relation = 'requires' and run_bridge.requirement_source = 'declared'"
            , order_by=concept_order
        ) }} as prerequisites_declared
        , {{ array_agg_filter(
            'concepts.name'
            , "run_bridge.relation = 'requires' and run_bridge.requirement_source = 'inferred'"
            , order_by=concept_order
        ) }} as prerequisites_inferred
    from run_bridge
    inner join concepts
        on run_bridge.concept_fk = concepts.concept_id
    group by run_bridge.courserun_fk
)

, statement_lists as (
    select
        courserun_fk
        , {{ array_agg_filter('label', "claim_kind = 'skill'", order_by='label', distinct=true) }} as skills
        , {{ array_agg_filter('label', "claim_kind = 'learning_outcome'", order_by='label', distinct=true) }} as learning_outcomes
        , {{ array_agg_filter('label', "claim_kind = 'audience'", order_by='label', distinct=true) }} as audience
    from claims
    group by courserun_fk
)

, knowledge as (
    select
        learn_runs.etl_source
        , learn_runs.readable_id
        , learn_runs.courserun_readable_id as extracted_courserun_readable_id
        , learn_runs.course_title
        , concept_lists.concepts_taught
        , concept_lists.concepts_reviewed
        , concept_lists.concepts_used
        , concept_lists.concepts_assessed
        , concept_lists.prerequisites_declared
        , concept_lists.prerequisites_inferred
        , statement_lists.skills
        , statement_lists.learning_outcomes
        , statement_lists.audience
    from learn_runs
    left join concept_lists
        on learn_runs.courserun_fk = concept_lists.courserun_fk
    left join statement_lists
        on learn_runs.courserun_fk = statement_lists.courserun_fk
)

select
    *
    , concat(
        '# ', course_title
        , {{ markdown_list_section('Teaches', 'concepts_taught') }}
        , {{ markdown_list_section('Reviews', 'concepts_reviewed') }}
        , {{ markdown_list_section('Applies', 'concepts_used') }}
        , {{ markdown_list_section('Learning outcomes', 'learning_outcomes', bullets=true) }}
        , {{ markdown_list_section('Skills', 'skills', bullets=true) }}
        , {{ markdown_list_section('Assessments cover', 'concepts_assessed') }}
        , {{ markdown_list_section('Stated prerequisites', 'prerequisites_declared') }}
        , {{ markdown_list_section('Preparation the material assumes', 'prerequisites_inferred') }}
        , {{ markdown_list_section('Audience', 'audience', bullets=true) }}
    ) as context_document
    , {{ courseware_build_id() }} as dbt_invocation_id
from knowledge
