-- The newest concept graph run, which the graph-position and community models read.
select graph_run_id
from (
    select
        graph_run_id
        , row_number() over (order by computed_at desc) as run_rank
    from {{ source('courseware_intermediate', 'courseware_concept_graph_run') }}
) as ranked
where run_rank = 1
