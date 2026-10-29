-- One row per concept graph run: how the graph's size, sparsity, connectivity,
-- community structure and cycle counts change as courses and resolution runs are
-- added. The history is the point; nothing here is filtered to the newest run.
select
    graph_run_id as concept_graph_run_pk
    , graph_version
    , computed_at
    , input_checksum
    , parameters_json
    , node_count
    , edge_count
    , edges_by_relation_json
    , density
    , sparsity
    , average_degree
    , component_count
    , largest_component_share
    , isolate_count
    , community_count
    , modularity
    , sequence_cycle_node_count
    , hierarchy_cycle_node_count
from {{ source('courseware_intermediate', 'courseware_concept_graph_run') }}
