{#
  integrations__learn__see_runs
  Runs of the Sloan Executive Education courses in integrations__learn__see_courses,
  one row per course offering, with the fields MIT Learn's legacy Sloan ETL set
  (transform_run). Dates are ISO 8601 UTC instants of midnight US Eastern.
  Contract: docs/learn_marts_contract.md
#}

with courses as (
    select readable_id from {{ ref('int__see__courses') }}
)

select
    runs.readable_id
    , runs.run_id
    , runs.title
    , runs.url
    , {{ format_timestamp_as_iso8601('runs.start_on') }} as start_date
    , {{ format_timestamp_as_iso8601('runs.end_on') }} as end_date
    , {{ array_of(['runs.delivery']) }} as delivery
    , runs.availability
    , {{ array_of(['runs.pace']) }} as pace
    , runs.format
    , runs.location
    , runs.price
    , runs.currency
    , runs.instructors
    , runs.duration
    , runs.min_weeks
    , runs.max_weeks
    , runs.time_commitment
    , runs.min_weekly_hours
    , runs.max_weekly_hours
    , 'Current' as status
    , true as published
from {{ ref('int__see__course_runs') }} as runs
inner join courses on runs.readable_id = courses.readable_id
