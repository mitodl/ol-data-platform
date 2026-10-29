{#
    Every sense of every normalized label, from the newest resolution run. A label's
    primary sense is the concept label resolution put it in; a split sense is a
    concept minted when a course run used the label for something else (e.g.
    "kernel" in an operating systems course and in support vector machines). An
    alias can name several concepts, and sense_gloss is what tells them apart.
#}
select alias.*
from {{ source('courseware_intermediate', 'courseware_concept_alias') }} as alias
inner join {{ ref('int__courseware__latest_resolution_run') }} as latest_run
    on alias.resolution_run_id = latest_run.resolution_run_id
