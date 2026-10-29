-- The concept relations resolution adjudication proposed (broader, close, related),
-- from the newest run's snapshot, with when each was first proposed.
with runs as (
    select * from {{ source('courseware_intermediate', 'courseware_resolution_run') }}
)

, latest_run as (
    select resolution_run_id
    from (
        select
            resolution_run_id
            , row_number() over (order by started_at desc) as run_rank
        from runs
    ) as ranked
    where run_rank = 1
)

, relations as (
    select relation.*
    from {{ source('courseware_intermediate', 'courseware_concept_relation') }} as relation
    inner join latest_run
        on relation.resolution_run_id = latest_run.resolution_run_id
)

select
    relations.from_concept_id
    , relations.to_concept_id
    , relations.relation
    , relations.proposed_run_id
    , runs.started_at as proposed_at
from relations
inner join runs
    on relations.proposed_run_id = runs.resolution_run_id
