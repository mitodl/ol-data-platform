{#
    Which concept each normalized label belongs to, from the newest resolution run:
    the label's primary sense. Claims do not join to concepts through this; a label
    can have several senses, and int__courseware__concept_sense_assignments says
    which one each course run uses.

    concept_id is an opaque UUID minted once by ml.lib.courseware.resolve and never
    derived from a label, so renaming a concept or re-running resolution keeps every
    join that uses it. Assignments always point at an active concept: the resolver
    redirects labels of merged concepts to the survivor. A label extracted since the
    last run has no assignment (concept_id null) until the next run resolves it.
#}
with labels as (
    select * from {{ ref('int__courseware__concept_labels') }}
)

, assignments as (
    select assignment.*
    from {{ source('courseware_intermediate', 'courseware_concept_assignment') }} as assignment
    inner join {{ ref('int__courseware__latest_resolution_run') }} as latest_run
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
