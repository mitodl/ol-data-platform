{#
    The concepts Learn can show to learners: one row per learner-facing concept with
    its descriptions and how many courses teach it, for concept chips, tooltips and a
    concept page. Pairs with integrations__learn__resource_concepts, which lists each
    course's concepts by id.

    Only accepted concepts are published, and course counts come only from relations
    an accepted claim supports; proposed rows stay in dim_concept and
    bridge_courserun_concept for review. No review workflow exists yet, so this is
    empty until one does.
#}
with concepts as (
    select * from {{ ref('dim_concept') }}
    where {{ learn_publishable_concept() }}
)

, run_bridge as (
    select * from {{ ref('bridge_courserun_concept') }}
    -- accepted claims only, and for inferred requires the support rule as well
    where accepted_claim_count > 0 and passes_support_rule
)

, course_runs as (
    select * from {{ ref('dim_course_run') }}
    where is_current
)

, course_counts as (
    select
        run_bridge.concept_fk
        , count(distinct case when run_bridge.relation = 'teaches' then course_runs.course_fk end) as teaching_course_count
        , count(distinct course_runs.course_fk) as course_count
    from run_bridge
    inner join course_runs
        on run_bridge.courserun_fk = course_runs.courserun_pk
    group by run_bridge.concept_fk
)

select
    concepts.concept_pk as concept_id
    , concepts.preferred_label as name
    , concepts.concept_kind
    , concepts.short_description
    , concepts.description
    , concepts.review_status
    , concepts.description_model_id
    , coalesce(course_counts.teaching_course_count, 0) as teaching_course_count
    , coalesce(course_counts.course_count, 0) as course_count
from concepts
left join course_counts
    on concepts.concept_pk = course_counts.concept_fk
