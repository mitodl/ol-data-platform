{{
    config(
        materialized='view'
    )
}}

{#
    Reporting view for problem events with denormalized course and problem information.
    Replaces Superset virtual dataset: Data_Detail_Problems
#}

with problem_events as (
    select * from {{ ref('tfact_problem_events') }}
)

, course_runs as (
    select
        courserun_readable_id
        , courserun_title as course_title
    from {{ ref('int__combined__course_runs') }}
)

, problems as (
    select
        problem_block_pk
        , problem_name
        , max_attempts
        , weight
        , markdown
    from {{ ref('dim_problem') }}
)

select
    problem_events.platform
    , course_runs.course_title
    , problem_events.courserun_readable_id
    , problems.problem_name
    , problem_events.event_type
    , problem_events.answers
    , problem_events.attempt
    , problem_events.success
    , problem_events.grade
    , problem_events.max_grade
    , problem_events.event_timestamp
    , problems.max_attempts
    , problems.weight
    , problems.markdown
    , problem_events.user_username
    , problem_events.openedx_user_id
    , problem_events.user_fk
from problem_events
left join problems
    on problem_events.problem_block_fk = problems.problem_block_pk
left join course_runs
    on problem_events.courserun_readable_id = course_runs.courserun_readable_id
