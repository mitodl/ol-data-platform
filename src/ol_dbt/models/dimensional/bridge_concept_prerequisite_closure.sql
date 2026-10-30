-- Experimental. Every concept each concept builds on, directly or through others,
-- over approved builds_on edges (review_status accepted) from the newest concept graph
-- run: one row per (from, to) with the fewest hops between them. A closure keeps the
-- meaning of its input edges and validates none of them, so it reads only edges a
-- reviewer approved for this purpose, never the observed builds_on candidates. No
-- review workflow exists yet, so it is empty. ml.lib.courseware.graph computes it with
-- NetworKit and stops at MAX_PREREQUISITE_DEPTH hops (parameters_json on
-- afact_concept_graph_run), an engineering bound, not evidence of anything. The
-- support rule does not gate an approved edge, so a pair approved in both directions
-- is a cycle, and an approved builds_on cycle fails the graph run, so the closure is a
-- strict partial order. An approved edge whose concept is no longer active is left
-- out and listed in afact_concept_graph_run.prerequisite_closure_excluded_json. Rows reflect bridge_concept_relation as of that run, not the latest build,
-- and are not yet pinned to a knowledge release.
with closure as (
    select closure.*
    from {{ source('courseware_intermediate', 'courseware_concept_graph_prerequisite_closure') }} as closure
    inner join {{ ref('int__courseware__latest_concept_graph_run') }} as latest
        on closure.graph_run_id = latest.graph_run_id
)

select
    from_concept_id as from_concept_fk
    , to_concept_id as to_concept_fk
    , min_depth
    , graph_run_id
from closure
