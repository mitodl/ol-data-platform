with
    problem_events as (select * from {{ ref("tfact_problem_events") }}),
    problems as (select * from {{ ref("dim_problem") }})

, course_runs as (
    {{ current_course_titles() }}
)

select
    problem_events.platform,
    course_runs.course_title,
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
    and problem_events.courserun_readable_id = problems.courserun_readable_id
inner join
    course_runs
    on problem_events.courserun_readable_id = course_runs.courserun_readable_id
    and problem_events.platform = course_runs.platform
group by
    problem_events.platform,
    course_runs.course_title,
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
