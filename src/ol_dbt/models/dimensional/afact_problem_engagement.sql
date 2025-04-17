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

, problem_attempt_aggregated as (
    select
        user_fk
        , platform
        , openedx_user_id
        , problem_block_fk
        , courserun_readable_id
        , max(attempt) as num_of_attempts
        , max(event_timestamp) as last_attempt_timestamp
        , count(case when success = 'correct' then 1 end) as num_of_correct_attempts
    from {{ ref('tfact_problem_events') }}
    where event_type = 'problem_check'
    group by platform, courserun_readable_id, problem_block_fk, openedx_user_id
)

, combined as (
    select
        problem_attempt_aggregated.user_fk
        , problem_attempt_aggregated.platform
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
    user_fk
    , platform
    , openedx_user_id
    , courserun_readable_id
    , problem_block_fk
    , num_of_attempts
    , num_of_correct_attempts
    , sequential_block_fk
    , chapter_block_fk
    , last_attempt_timestamp
from combined
