with problem_structure as (
    select
        courserun_readable_id
        , block_id as problem_block_fk
        , sequential_block_id as sequential_block_fk
        , chapter_block_id as chapter_block_fk
    from {{ ref('dim_course_content') }}
    where
        block_category = 'problem'
        and is_latest = true
)

, pre_problem_attempt_aggregated as (
    select
        platform
        , openedx_user_id
        , problem_block_fk
        , courserun_readable_id
        , attempt
        , event_timestamp
        , success
        , row_number() 
            over 
            (
                partition by platform, openedx_user_id, courserun_readable_id, problem_block_fk, attempt 
                order by event_timestamp desc
            ) 
        as rn
    from {{ ref('tfact_problem_events') }}
    where event_type = 'problem_check'
)

, problem_attempt_aggregated as (
    select
        platform
        , openedx_user_id
        , problem_block_fk
        , courserun_readable_id
        , max(attempt) as num_of_attempts
        , max(event_timestamp) as last_attempt_timestamp
        , count(case when success = 'correct' then 1 end) as num_of_correct_attempts
    from pre_problem_attempt_aggregated
    where rn = 1
    group by platform, courserun_readable_id, problem_block_fk, openedx_user_id
)

, combined as (
    select
        problem_attempt_aggregated.platform
        , problem_attempt_aggregated.openedx_user_id
        , problem_attempt_aggregated.courserun_readable_id
        , problem_attempt_aggregated.problem_block_fk
        , problem_attempt_aggregated.num_of_attempts
        , problem_attempt_aggregated.num_of_correct_attempts
        , problem_attempt_aggregated.last_attempt_timestamp
        , problem_structure.sequential_block_fk
        , problem_structure.chapter_block_fk
    from problem_attempt_aggregated
    left join problem_structure
        on problem_attempt_aggregated.problem_block_fk = problem_structure.problem_block_fk
)

select
    platform
    , openedx_user_id
    , courserun_readable_id
    , problem_block_fk
    , num_of_attempts
    , num_of_correct_attempts
    , sequential_block_fk
    , chapter_block_fk
    , last_attempt_timestamp
from combined
