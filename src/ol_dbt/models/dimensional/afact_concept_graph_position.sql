-- Each active concept's place in the newest concept graph: its community, how
-- connected it is, how foundational (PageRank over builds_on, where rank flows to
-- what others build on), how deep its builds_on chain (observed instructional
-- order, not prerequisites) and hierarchy go, and whether it sits on a cycle (a
-- finding to review).
with nodes as (
    select node.*
    from {{ source('courseware_intermediate', 'courseware_concept_graph_node') }} as node
    inner join {{ ref('int__courseware__latest_concept_graph_run') }} as latest
        on node.graph_run_id = latest.graph_run_id
)

select
    concept_id as concept_fk
    , community_id as community_fk
    , component_index
    , degree
    , weighted_degree
    , foundation_rank
    , sequence_level
    , hierarchy_depth
    , in_sequence_cycle
    , in_hierarchy_cycle
    , graph_run_id
from nodes
