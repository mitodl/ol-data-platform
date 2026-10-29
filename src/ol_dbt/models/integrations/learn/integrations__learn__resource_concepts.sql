{#
    Which concepts each Learn course teaches, reviews, uses, assesses or requires,
    one row per (course, concept, relation), from the run
    int__courseware__learn_course_runs selects. concept_id is dim_concept's UUID and
    is already the sense the course uses: two courses that use "kernel" for two
    meanings point at two concepts, so Learn joins its copy to the concept graph by
    id and never resolves labels again. integrations__learn__concepts has each
    concept's name and descriptions.

    Only concepts integrations__learn__concepts publishes (accepted, active,
    learner-facing, with current labels), and relations at least one accepted claim
    supports, are published, so every concept_id here has a row there. No review
    workflow exists yet, so this is empty until one does.

    The release manifest will add a release id to every row, binding this to the
    resolution run, sense assignments and graph run it was built from; until then
    the rows are the newest resolution run's.
#}
with learn_runs as (
    select * from {{ ref('int__courseware__learn_course_runs') }}
)

, run_bridge as (
    select * from {{ ref('bridge_courserun_concept') }}
    where accepted_claim_count > 0
)

, concepts as (
    select * from {{ ref('dim_concept') }}
    where {{ learn_publishable_concept() }}
)

select
    learn_runs.etl_source
    , learn_runs.readable_id
    , learn_runs.courserun_readable_id as extracted_courserun_readable_id
    , concepts.concept_pk as concept_id
    , concepts.preferred_label as concept_name
    , run_bridge.relation
    , run_bridge.requirement_source
    , run_bridge.accepted_claim_count as claim_count
    , run_bridge.first_scope_position
from learn_runs
inner join run_bridge
    on learn_runs.courserun_fk = run_bridge.courserun_fk
inner join concepts
    on run_bridge.concept_fk = concepts.concept_pk
