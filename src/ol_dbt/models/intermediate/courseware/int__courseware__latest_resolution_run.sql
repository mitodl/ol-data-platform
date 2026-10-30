-- The newest concept resolution run, whose snapshots the concept models read.
select resolution_run_id
from (
    select
        resolution_run_id
        , row_number() over (order by started_at desc) as run_rank
    from {{ source('courseware_intermediate', 'courseware_resolution_run') }}
) as ranked
where run_rank = 1
