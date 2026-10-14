with
    problem_events as (select * from {{ ref("tfact_problem_events") }}),
    problems as (select * from {{ ref("dim_problem") }})

-- dim_course_run and dim_course are SCD2 and can hold more than one is_current = true row per
-- key, so keep the latest. Runs with no dim_course row (all residential runs) use the run title.
, course_runs as (
    select
        courserun_readable_id
        , platform
        , course_title
    from (
        select
            dim_course_run.courserun_readable_id
            , dim_course_run.platform
            , coalesce(dim_course.course_title, dim_course_run.courserun_title) as course_title
            , row_number() over (
                partition by dim_course_run.platform, dim_course_run.courserun_readable_id
                order by
                    dim_course_run.effective_date desc nulls last
                    , dim_course.effective_date desc nulls last
            ) as _row_num
        from {{ ref('dim_course_run') }} as dim_course_run
        left join {{ ref('dim_course') }} as dim_course
            on
                dim_course_run.course_fk = dim_course.course_pk
                and dim_course.is_current = true
        where dim_course_run.is_current = true
    )
    where _row_num = 1
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
