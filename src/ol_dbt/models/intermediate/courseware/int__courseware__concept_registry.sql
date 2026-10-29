{#
    Every concept id ever minted, as of the newest resolution run: active, merged
    into a survivor (merged_into_concept_id, already resolved to an active concept),
    or deprecated. Retired ids stay so anything that stored one can follow the
    redirect.
#}
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

select registry.*
from {{ source('courseware_intermediate', 'courseware_concept_registry') }} as registry
inner join latest_run
    on registry.resolution_run_id = latest_run.resolution_run_id
