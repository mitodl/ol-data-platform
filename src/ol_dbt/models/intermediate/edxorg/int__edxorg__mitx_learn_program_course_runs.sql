{#
  One row per course run of each program course in the latest edX programs API
  extraction, as the programs API reports it. MIT Learn derives a program's dates,
  price, pace and availability from these runs (learning_resources/etl/openedx.py).
  The raw table only appends, so only rows from the latest extraction are current.
#}

with program_courses as (
    select *
    from {{ ref('stg__edxorg__s3__program_courses') }}
    where program_course_retrieved_at = (
        select max(program_course_retrieved_at) from {{ ref('stg__edxorg__s3__program_courses') }}
    )
)

, runs as (
    select
        program_courses.program_uuid
        , program_courses.course_key
        , {{ json_extract_scalar('course_run.run', "'$.key'") }} as run_key
        , {{ json_extract_scalar('course_run.run', "'$.start'") }} as run_start_on
        , {{ json_extract_scalar('course_run.run', "'$.end'") }} as run_end_on
        , {{ json_extract_scalar('course_run.run', "'$.enrollment_start'") }} as run_enrollment_start_on
        , {{ json_extract_scalar('course_run.run', "'$.enrollment_end'") }} as run_enrollment_end_on
        , {{ json_extract_scalar('course_run.run', "'$.status'") }} as run_status
        , {{ json_extract_scalar('course_run.run', "'$.is_enrollable'") }} = 'true' as run_is_enrollable
        , {{ json_extract_scalar('course_run.run', "'$.pacing_type'") }} as run_pacing_type
        , {{ json_extract_scalar('course_run.run', "'$.availability'") }} as run_availability
        , {{ json_extract_scalar('course_run.run', "'$.seats[0].currency'") }} as run_first_seat_currency
        , {{ json_array_string('course_run.run', "'$.seats'") }} as run_seats_json
    from program_courses
    cross join {{ unnest_json_array('program_courses.course_runs_json', 'course_run', 'run') }}
)

, seat_prices as (
    select
        runs.program_uuid
        , runs.course_key
        , runs.run_key
        , min(cast({{ json_extract_scalar('seat.seat', "'$.price'") }} as decimal(12, 2))) as run_min_paid_price
    from runs
    cross join {{ unnest_json_array('runs.run_seats_json', 'seat', 'seat') }}
    -- matches the literal price string, as MIT Learn did
    where {{ json_extract_scalar('seat.seat', "'$.price'") }} != '0.00'
    group by runs.program_uuid, runs.course_key, runs.run_key
)

select
    runs.program_uuid
    , runs.course_key
    , runs.run_key
    , runs.run_start_on
    , runs.run_end_on
    , runs.run_enrollment_start_on
    , runs.run_enrollment_end_on
    , runs.run_status
    , runs.run_pacing_type
    , runs.run_availability
    -- MIT Learn counts a run as published only when edX both publishes it and opens
    -- enrollment
    , coalesce(runs.run_status = 'published' and runs.run_is_enrollable, false) as run_is_published
    , seat_prices.run_min_paid_price
    , runs.run_first_seat_currency
from runs
left join seat_prices
    on runs.program_uuid = seat_prices.program_uuid
    and runs.course_key = seat_prices.course_key
    and runs.run_key = seat_prices.run_key
