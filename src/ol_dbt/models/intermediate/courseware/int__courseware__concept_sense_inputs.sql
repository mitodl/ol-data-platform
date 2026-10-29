{#
    What sense assignment (ml.lib.courseware.senses) reads: every concept mention
    with its course run and course title. A label's sense is decided once per course
    run, from the run's course title and its claims' descriptions of the label.
#}
with mentions as (
    select * from {{ ref('int__courseware__concept_mentions') }}
)

, course_runs as (
    select * from {{ ref('dim_course_run') }}
    where is_current
)

, courses as (
    select * from {{ ref('dim_course') }}
    where is_current
)

select
    mentions.concept_label_key
    , mentions.platform
    , mentions.courserun_readable_id
    , courses.course_title
    , mentions.label
    , mentions.concept_kind
    , mentions.description
    , mentions.teaching_status
from mentions
left join course_runs
    on
        mentions.platform = course_runs.platform
        and mentions.courserun_readable_id = course_runs.courserun_readable_id
left join courses
    on course_runs.course_fk = courses.course_pk
