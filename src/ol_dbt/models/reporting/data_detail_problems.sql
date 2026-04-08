with
    problem_events as (select * from {{ ref("tfact_problem_events") }}),
    problems as (select * from {{ ref("dim_problem") }}),
    combined__course_runs as (select * from {{ ref("int__combined__course_runs") }})

select
    problem_events.platform,
    combined__course_runs.course_title,
    problem_events.courserun_readable_id,
    problem_events.user_username,
    problems.problem_name,
    problem_events.event_type,
    problem_events.answers,
    problem_events.attempt,
    problem_events.success,
    problem_events.grade,
    problem_events.max_grade,
    problem_events.event_timestamp,
    problems.max_attempts,
    problems.weight,
    problems.markdown
from problem_events
inner join
    problems
    on problem_events.problem_block_fk = problems.problem_block_pk
    and problem_events.platform = problems.platform
inner join
    combined__course_runs
    on problem_events.courserun_readable_id = combined__course_runs.courserun_readable_id
    and problem_events.platform = combined__course_runs.platform
group by
    problem_events.platform,
    combined__course_runs.course_title,
    problem_events.courserun_readable_id,
    problem_events.user_username,
    problems.problem_name,
    problem_events.event_type,
    problem_events.answers,
    problem_events.attempt,
    problem_events.success,
    problem_events.grade,
    problem_events.max_grade,
    problem_events.event_timestamp,
    problems.max_attempts,
    problems.weight,
    problems.markdown
