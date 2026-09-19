{#
  One row per run of an MIT Professional Education course or program. The feed gives
  runs as pipe-separated lists aligned by position; like Python's zip(), a run exists
  only where all four lists have an entry. Dates are calendar dates in US Eastern time.
#}

with source as (
    select * from {{ ref('stg__mitpe__api__courses') }}
)

, run_lists as (
    select
        course_uuid
        , split(coalesce(course_run_ids_raw, ''), '|') as run_ids
        , split(coalesce(course_run_start_dates_raw, ''), '|') as start_dates
        , split(coalesce(course_run_end_dates_raw, ''), '|') as end_dates
        , split(coalesce(course_run_enrollment_end_dates_raw, ''), '|') as enrollment_end_dates
    from source
)

, runs as (
    select
        run_lists.course_uuid as readable_id
        , run_index.run_position
        , {{ element_at_array('run_lists.run_ids', 'run_index.run_position') }} as run_id
        , nullif({{ element_at_array('run_lists.start_dates', 'run_index.run_position') }}, '') as start_date
        , nullif({{ element_at_array('run_lists.end_dates', 'run_index.run_position') }}, '') as end_date
        , nullif({{ element_at_array('run_lists.enrollment_end_dates', 'run_index.run_position') }}, '')
            as enrollment_end_date
    from run_lists
    cross join {{ unnest_sequence(
        'least(' ~ array_length('run_lists.run_ids') ~ ', ' ~ array_length('run_lists.start_dates') ~ ', '
        ~ array_length('run_lists.end_dates') ~ ', ' ~ array_length('run_lists.enrollment_end_dates') ~ ')',
        'run_index', 'run_position'
    ) }}
)

, dated_runs as (
    select
        readable_id
        , run_position
        , run_id
        , {{ local_date_to_utc('start_date', 'America/New_York') }} as start_on
        , {{ local_date_to_utc('end_date', 'America/New_York') }} as end_on
        , {{ local_date_to_utc('enrollment_end_date', 'America/New_York') }} as enrollment_end_on
    from runs
)

select
    readable_id
    , run_position
    , run_id
    , start_on
    , end_on
    , enrollment_end_on
    -- A run is live while enrollment (or, failing that, the run itself) hasn't ended.
    -- A run with neither date never expires. Evaluated when the model is built.
    , (
        run_id != ''
        and (
            (end_on is null and enrollment_end_on is null)
            or {{ utc_now() }} <= coalesce(enrollment_end_on, end_on)
        )
    ) as is_published
from dated_runs
