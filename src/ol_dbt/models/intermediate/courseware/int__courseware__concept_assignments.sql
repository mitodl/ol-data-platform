{#
    Which concept each normalized label belongs to, from the newest resolution run.
    concept_id is an opaque UUID minted once by ml.lib.courseware.resolve and never
    derived from a label, so renaming a concept or re-running resolution keeps every
    join that uses it. Assignments always point at an active concept: the resolver
    redirects labels of merged concepts to the survivor. A label extracted since the
    last run has no assignment (concept_id null) until the next run resolves it.
#}
with labels as (
    select * from {{ ref('int__courseware__concept_labels') }}
)

, runs as (
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

, assignments as (
    select assignment.*
    from {{ source('courseware_intermediate', 'courseware_concept_assignment') }} as assignment
    inner join latest_run
        on assignment.resolution_run_id = latest_run.resolution_run_id
)

select
    labels.concept_label_key
    , labels.preferred_label
    , labels.mention_count
    , labels.courserun_count
    , assignments.concept_id
    , assignments.assignment_source
    , assignments.assigned_run_id
    , assignments.resolution_run_id
from labels
left join assignments
    on labels.concept_label_key = assignments.concept_label_key
