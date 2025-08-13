with problem_events as (
    select * from {{ ref('tfact_problem_events') }}
)

, enrollment_detail as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, course_content as (
    select * from {{ ref('dim_course_content') }}
)

, overall_grade as (
    select
        user_id
        , courserun_readable_id
        , max(courserungrade_grade) as courserungrade_grade
    from enrollment_detail
    group by
        user_id
        , courserun_readable_id
)

, problems_joined as (
    select
        problem_events.platform
        , problem_events.openedx_user_id
        , problem_events.courserun_readable_id
        , problem_events.problem_block_fk
        , problem_events.event_timestamp
        , problem_events.grade
        , problem_events.max_grade
        , problem_events.attempt
        , overall_grade.courserungrade_grade
        , unit.block_title as unit_name
        , chapter.block_title as chapter_name
        , lag(problem_events.event_timestamp, 1)
            over (
                partition by
                    problem_events.platform
                    , problem_events.openedx_user_id
                    , problem_events.courserun_readable_id
                order by problem_events.event_timestamp
            ) as prev_event_timestamp
    from problem_events
    left join overall_grade
        on
            cast(problem_events.openedx_user_id as varchar) = overall_grade.user_id
            and problem_events.courserun_readable_id = overall_grade.courserun_readable_id
    left join course_content as cc
        on
            problem_events.problem_block_fk = cc.block_id
            and cc.is_latest = true
    left join course_content as unit
        on
            cc.parent_block_id = unit.block_id
            and unit.is_latest = true
    left join course_content as chapter
        on
            cc.chapter_block_id = chapter.block_id
            and chapter.is_latest = true
)

select
    platform
    , openedx_user_id
    , courserun_readable_id
    , problem_block_fk
    , unit_name
    , chapter_name
    , max(max_grade) as max_grade
    , max(attempt) as attempts_on_problem
    , array_agg(grade) as grades
    , min(
        case
            when event_timestamp - prev_event_timestamp < interval '600' second
                then event_timestamp - prev_event_timestamp
        end
    ) as time_spent_on_problem
    , max(courserungrade_grade) as courserungrade_grade
from problems_joined
group by
    platform
    , openedx_user_id
    , courserun_readable_id
    , problem_block_fk
    , unit_name
    , chapter_name
