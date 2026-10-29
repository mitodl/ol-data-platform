{#
    What each Learn course teaches, assesses and expects, from its most recently
    extracted run, keyed as Learn keys the course (etl_source, readable_id).

    context_document renders the same arrays as markdown, deterministically, so it can
    be embedded as a resource's search text (the embedding substrate's
    resource_knowledge_markdown_v1 builder) without another model call.

    Only accepted concepts and accepted claims are published: a concept appears in a
    course's arrays only when integrations__learn__concepts publishes it (accepted,
    active, learner-facing, with current labels) and an accepted claim supports the
    relation, since accepting a concept does not accept the claims naming it.
    Proposed rows stay in dim_concept, bridge_courserun_concept and
    tfact_courseware_claim for review. No review workflow exists
    yet, so the concept and statement arrays are null until one does.
#}
with extractions as (
    select * from {{ ref('dim_courseware_extraction') }}
    where is_current
)

, course_runs as (
    select * from {{ ref('dim_course_run') }}
    where is_current
)

, courses as (
    select * from {{ ref('dim_course') }}
    where is_current
)

, run_bridge as (
    select * from {{ ref('bridge_courserun_concept') }}
    where accepted_claim_count > 0
)

, concepts as (
    select * from {{ ref('dim_concept') }}
    where {{ learn_publishable_concept() }}
)

, claims as (
    select * from {{ ref('tfact_courseware_claim') }}
    where is_current and is_verified and review_status = 'accepted'
)

-- the course's run with the newest extraction
, extracted_runs as (
    select
        platform
        , courserun_readable_id
        , courserun_fk
        , row_number() over (
            partition by course_fk order by last_extracted_at desc
        ) as run_rank
    from (
        select
            extractions.platform
            , extractions.courserun_readable_id
            , extractions.courserun_fk
            , course_runs.course_fk
            , max(extractions.extracted_at) as last_extracted_at
        from extractions
        inner join course_runs
            on extractions.courserun_fk = course_runs.courserun_pk
        group by
            extractions.platform
            , extractions.courserun_readable_id
            , extractions.courserun_fk
            , course_runs.course_fk
    ) as runs
)

, concept_lists as (
    select
        run_bridge.courserun_fk
        , array_agg(concepts.preferred_label order by run_bridge.claim_count desc, concepts.preferred_label ASC) filter (where run_bridge.relation = 'teaches') as concepts_taught
        , array_agg(concepts.preferred_label order by run_bridge.claim_count desc, concepts.preferred_label ASC) filter (where run_bridge.relation = 'reviews') as concepts_reviewed
        , array_agg(concepts.preferred_label order by run_bridge.claim_count desc, concepts.preferred_label ASC) filter (where run_bridge.relation = 'uses') as concepts_used
        , array_agg(concepts.preferred_label order by run_bridge.claim_count desc, concepts.preferred_label ASC) filter (where run_bridge.relation = 'assesses') as concepts_assessed
        , array_agg(concepts.preferred_label order by run_bridge.claim_count desc, concepts.preferred_label ASC) filter (
            where run_bridge.relation = 'requires' and run_bridge.requirement_source = 'declared'
        ) as prerequisites_declared
        , array_agg(concepts.preferred_label order by run_bridge.claim_count desc, concepts.preferred_label ASC) filter (
            where run_bridge.relation = 'requires' and run_bridge.requirement_source = 'inferred'
        ) as prerequisites_inferred
    from run_bridge
    inner join concepts
        on run_bridge.concept_fk = concepts.concept_pk
    group by run_bridge.courserun_fk
)

, statement_lists as (
    select
        courserun_fk
        , array_agg(distinct label order by label) filter (where claim_kind = 'skill') as skills
        , array_agg(distinct label order by label) filter (where claim_kind = 'learning_outcome') as learning_outcomes
        , array_agg(distinct label order by label) filter (where claim_kind = 'audience') as audience
    from claims
    group by courserun_fk
)

, knowledge as (
    select
        case extracted_runs.platform
            when 'mitxonline' then 'mitxonline'
            when 'mitxpro' then 'xpro'
        end as etl_source
        , courses.course_readable_id as readable_id
        , extracted_runs.courserun_readable_id as extracted_courserun_readable_id
        , courses.course_title
        , concept_lists.concepts_taught
        , concept_lists.concepts_reviewed
        , concept_lists.concepts_used
        , concept_lists.concepts_assessed
        , concept_lists.prerequisites_declared
        , concept_lists.prerequisites_inferred
        , statement_lists.skills
        , statement_lists.learning_outcomes
        , statement_lists.audience
    from extracted_runs
    inner join course_runs
        on extracted_runs.courserun_fk = course_runs.courserun_pk
    inner join courses
        on course_runs.course_fk = courses.course_pk
    left join concept_lists
        on extracted_runs.courserun_fk = concept_lists.courserun_fk
    left join statement_lists
        on extracted_runs.courserun_fk = statement_lists.courserun_fk
    where extracted_runs.run_rank = 1
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
from knowledge
where etl_source is not null
