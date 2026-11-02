{#
    Every concept id ever minted, as of the newest resolution run: active, merged
    into a survivor (merged_into_concept_id, already resolved to an active concept),
    or deprecated. Retired ids stay so anything that stored one can follow the
    redirect. A concept split off a label for another meaning records the concept it
    was split from (split_from_concept_id).
#}
select registry.*
from {{ source('courseware_intermediate', 'courseware_concept_registry') }} as registry
inner join {{ ref('int__courseware__latest_resolution_run') }} as latest_run
    on registry.resolution_run_id = latest_run.resolution_run_id
