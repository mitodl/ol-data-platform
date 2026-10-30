{#
    Which concepts each Learn course teaches, reviews, uses, assesses or requires,
    one row per (course, concept, relation), as the next knowledge release would
    publish them, from the course's run in int__courseware__learn_course_runs. The
    courseware_knowledge_release asset copies these rows into a release, and
    integrations__learn__resource_concepts reads them from the published one.

    concept_id is dim_concept's UUID and is already the sense the course uses: two
    courses that use "kernel" for two meanings point at two concepts, so Learn joins
    its copy to the concept graph by id and never resolves labels again.

    Only concepts int__courseware__release_concepts publishes as learner-facing are
    listed, so every concept_id is in the same release's integrations__learn__concepts,
    and only relations an accepted claim supports; an inferred requirement must also
    pass the support rule, which filters noise and does not make it a reviewed
    prerequisite. These are the rows int__courseware__release_resource_knowledge
    renders into context_document.
#}
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

select
    learn_runs.etl_source
    , learn_runs.readable_id
    , learn_runs.courserun_readable_id as extracted_courserun_readable_id
    , concepts.concept_id
    , concepts.name as concept_name
    , run_bridge.relation
    , run_bridge.requirement_source
    , run_bridge.accepted_claim_count as claim_count
    , run_bridge.first_scope_position
    , {{ courseware_build_id() }} as dbt_invocation_id
from learn_runs
inner join run_bridge
    on learn_runs.courserun_fk = run_bridge.courserun_fk
inner join concepts
    on run_bridge.concept_fk = concepts.concept_id
