{#
  One row per run of an MIT Professional Education course or program. The feed gives
  runs as pipe-separated lists aligned by position. A run exists wherever any of the
  four lists has an entry, and a list too short to reach it leaves that field null;
  is_in_all_run_lists marks the runs every list reaches (MIT Learn's ETL zips the
  lists and keeps only those). Dates are calendar dates in US Eastern time.
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

, run_list_lengths as (
    select
        course_uuid
        , run_ids
        , start_dates
        , end_dates
        , enrollment_end_dates
        , least(
            {{ array_length('run_ids') }}, {{ array_length('start_dates') }}
            , {{ array_length('end_dates') }}, {{ array_length('enrollment_end_dates') }}
        ) as shortest_length
        , greatest(
            {{ array_length('run_ids') }}, {{ array_length('start_dates') }}
            , {{ array_length('end_dates') }}, {{ array_length('enrollment_end_dates') }}
        ) as longest_length
    from run_lists
)

, runs as (
    select
        run_lists.course_uuid as readable_id
        , run_index.run_position
        , run_index.run_position <= run_lists.shortest_length as is_in_all_run_lists
        , {{ element_at_array('run_lists.run_ids', 'run_index.run_position') }} as run_id
        , nullif({{ element_at_array('run_lists.start_dates', 'run_index.run_position') }}, '') as start_date
        , nullif({{ element_at_array('run_lists.end_dates', 'run_index.run_position') }}, '') as end_date
        , nullif({{ element_at_array('run_lists.enrollment_end_dates', 'run_index.run_position') }}, '')
            as enrollment_end_date
    from run_list_lengths as run_lists
    cross join {{ unnest_sequence('run_lists.longest_length', 'run_index', 'run_position') }}
)

, dated_runs as (
    select
        readable_id
        , run_position
        , is_in_all_run_lists
        , run_id
        , {{ local_date_to_timestamptz('start_date', 'America/New_York') }} as start_on
        , {{ local_date_to_timestamptz('end_date', 'America/New_York') }} as end_on
        , {{ local_date_to_timestamptz('enrollment_end_date', 'America/New_York') }} as enrollment_end_on
    from runs
)

select
    readable_id
    , run_position
    , is_in_all_run_lists
    , run_id
    , start_on
    , end_on
    , enrollment_end_on
    -- A run is live while enrollment (or, failing that, the run itself) hasn't ended.
    -- A run with neither date never expires. Evaluated when the model is built.
    , (
        coalesce(run_id, '') != ''
        and (
            (end_on is null and enrollment_end_on is null)
            or current_timestamp <= coalesce(enrollment_end_on, end_on)
        )
    ) as is_published
from dated_runs
