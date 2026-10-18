-- Every concept each concept builds on, directly or through others, from the newest
-- concept graph run: one row per (from, to) with the fewest published builds_on
-- hops between them. ml.lib.courseware.graph computes it with NetworKit and stops at
-- MAX_PREREQUISITE_DEPTH hops (parameters_json on afact_concept_graph_run), so a
-- concept whose sequence_level exceeds the cap has prerequisites this leaves out. A
-- published builds_on cycle fails the graph run, so the closure is a strict partial
-- order. Rows reflect bridge_concept_relation as of that run, not the latest build.
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
