with course_problems as (
    select
        courserun_readable_id
        , count(distinct problem_id) as num_of_problems
    from tfact_problem_events
    group by courserun_readable_id
)

, course_sections as (
    select
        courserun_readable_id
        , parent_block_id as section_block_id
        , parent.block_title as section_name
        , block_id as subsection_block_id
        , block_title as subsection_name
    from dim_course_content
    left join dim_course_content as parent
        on dim_course_content.parent_block_id = parent.block_id
    where block_category = 'problem'
)

, problems_attempted as (
    select
        user_id
        , courserun_readable_id
        , count(distinct problem_id) as num_of_problems_attempted
        , count(*) as total_num_of_attempts
        , 100.0 * sum(case
            when success = 'correct' then 1
            else 0
        end) / count(*) as problem_attempts_correct
        , sum(cast(grade as double)) / nullif(sum(cast(max_grade as double)), 0) as problem_attempts_grade
        , problem_block_id
    from tfact_problem_events
    where event_type = 'problem_check'
    group by user_id, courserun_readable_id
)

, combined as (
    select
       problems_attempted.user_id,
        , problems_attempted.courserun_readable_id
        , problems_attempted.problem_block_id as section_block_id
        , course_sections.section_name
        , course_sections.subsection_block_id
        , course_sections.subsection_name
        , course_problems.num_of_problems
        , problems_attempted.num_of_problems_attempted
        , problems_attempted.total_num_of_attempts
        , problems_attempted.problem_attempts_correct
        , problems_attempted.problem_attempts_grade
    from problems_attempted
    left join course_problems
        on problems_attempted.courserun_readable_id = course_problems.courserun_readable_id
    left join course_sections
        on problems_attempted.problem_block_id = course_sections.subsection_block_id
)

select distinct
    user_id
    , courserun_readable_id
    , section_block_id
    , section_name
    , subsection_block_id
    , subsection_name
    , num_of_problems
    , num_of_problems_attempted,
    , total_num_of_attempts
    , problem_attempts_correct
    , problem_attempts_grade
from combined
