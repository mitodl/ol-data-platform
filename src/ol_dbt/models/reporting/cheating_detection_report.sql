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
        , sum(case when courserunenrollment_enrollment_mode = 'verified' then 1 else 0 end) as verified_cnt
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
        , sequential.block_title as sequential_name
        , lag(problem_events.event_timestamp, 1)
            over (
                partition by
                    problem_events.platform
                    , problem_events.openedx_user_id
                    , problem_events.courserun_readable_id
                    , cc.parent_block_id
                order by problem_events.event_timestamp
            ) as prev_event_timestamp
    from problem_events
    inner join overall_grade
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
    left join course_content as sequential
        on
            cc.sequential_block_id = sequential.block_id
            and sequential.is_latest = true
    where overall_grade.verified_cnt > 0
)

, final as (
    select
        platform
        , openedx_user_id
        , courserun_readable_id
        , problem_block_fk
        , unit_name
        , chapter_name
        , sequential_name
        , coalesce((
            upper(unit_name) like '%EXAM%'
            and upper(unit_name) not like '%EXAMPLE%'
            and upper(unit_name) not like '%EXAMINING%'
            and upper(unit_name) not like '%PRACTICE%'
        )
        or (
            upper(chapter_name) like '%EXAM%'
            and upper(chapter_name) not like '%EXAMPLE%'
            and upper(chapter_name) not like '%EXAMINING%'
            and upper(chapter_name) not like '%PRACTICE%'
        )
        or upper(chapter_name) like '%EXAM %'
        or upper(unit_name) like '%EXAM %', true
        ) as exam_indicator
        , coalesce((upper(unit_name) like '%HOMEWORK%'
        or upper(chapter_name) like '%HOMEWORK%'), true
        ) as hw_indicator
        , max(max_grade) as max_possible_grade
        , max(attempt) as attempts_on_problem
        , max(grade) as max_learner_grade
        , avg(grade) as avg_learner_grade
        , array_agg(grade) as grades
        , min(
            case
                when
                    prev_event_timestamp is not null
                    and date_diff('second', prev_event_timestamp, event_timestamp) < 600
                    then date_diff('second', prev_event_timestamp, event_timestamp)
            end
        ) as time_spent_on_problem
        , min(
            case
                when
                    prev_event_timestamp is not null
                    then date_diff('second', prev_event_timestamp, event_timestamp)
            end
        ) as time_spent_on_problem_nolimit
        , max(courserungrade_grade) as courserungrade_grade
    from problems_joined
    group by
        platform
        , openedx_user_id
        , courserun_readable_id
        , problem_block_fk
        , unit_name
        , chapter_name
        , sequential_name
        , coalesce((
            upper(unit_name) like '%EXAM%'
            and upper(unit_name) not like '%EXAMPLE%'
            and upper(unit_name) not like '%EXAMINING%'
            and upper(unit_name) not like '%PRACTICE%'
        )
        or (
            upper(chapter_name) like '%EXAM%'
            and upper(chapter_name) not like '%EXAMPLE%'
            and upper(chapter_name) not like '%EXAMINING%'
            and upper(chapter_name) not like '%PRACTICE%'
        )
        or upper(chapter_name) like '%EXAM %'
        or upper(unit_name) like '%EXAM %', true
        )
        , coalesce((upper(unit_name) like '%HOMEWORK%'
            or upper(chapter_name) like '%HOMEWORK%'), true
        )
)

, ten_percent_time as (
    select
        platform
        , courserun_readable_id
        , problem_block_fk
        , approx_percentile(time_spent_on_problem, 0.1) as time_spent_percentile_10
    from final
    group by
        platform
        , courserun_readable_id
        , problem_block_fk
)

, ten_percent_time as (
    select
        platform
        , courserun_readable_id
        , problem_block_fk
        , approx_percentile(time_spent_on_problem, 0.1) as time_spent_percentile_10
    from final
    group by
        platform
        , courserun_readable_id
        , problem_block_fk
)

select
    final.platform
    , final.openedx_user_id
    , final.courserun_readable_id
    , final.problem_block_fk
    , final.unit_name
    , final.chapter_name
    , final.sequential_name
    , final.exam_indicator
    , final.hw_indicator
    , final.max_possible_grade
    , final.attempts_on_problem
    , final.max_learner_grade
    , final.avg_learner_grade
    , final.grades
    , final.time_spent_on_problem
    , final.time_spent_on_problem_nolimit
    , final.courserungrade_grade
    , approx_percentile(time_spent_on_problem, 0.5)
    , case when final.time_spent_on_problem < ten_percent_time.time_spent_percentile_10
        then 1 else 0 end time_flag
from final
left join ten_percent_time
    on final.problem_block_fk = ten_percent_time.problem_block_fk
