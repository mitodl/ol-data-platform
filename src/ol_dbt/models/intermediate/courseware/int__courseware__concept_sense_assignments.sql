{#
    Which sense of its label each course run uses, from the newest resolution run,
    one row per (concept_label_key, platform, courserun_readable_id). This is how
    claims join to concepts: two courses can use one label for two concepts.

    sense_status needs_review (concept_id null) is the review queue: the model
    could not tell which of the label's senses the run means, and nothing falls
    back to the first sense. A (label, course run) the newest run has not seen has
    no row until the next run assigns it.
#}
select assignment.*
from {{ source('courseware_intermediate', 'courseware_concept_sense_assignment') }} as assignment
inner join {{ ref('int__courseware__latest_resolution_run') }} as latest_run
    on assignment.resolution_run_id = latest_run.resolution_run_id
