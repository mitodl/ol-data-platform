with problem_engagement as (
    select * from {{ ref('afact_problem_engagement') }}
)

, dim_user as (
    select * from {{ ref('dim_user') }}
)

, problem_grades as (
    select
        platform
        , problem_block_fk
        , courserun_readable_id
        , openedx_user_id
        , max(cast(max_grade as decimal(30, 10))) as max_possible_grade
        , min(cast(grade as decimal(30, 10))) as learner_lowest_grade
        , max(cast(grade as decimal(30, 10))) as learner_highest_grade
    from {{ ref('tfact_problem_events') }}
    group by
        platform
        , problem_block_fk
        , courserun_readable_id
        , openedx_user_id
)

, problems_per_chapter as (
    select
        chapter_block_id as chapter_block_fk
        , count(distinct block_id) as number_of_problems
    from {{ ref('dim_course_content') }}
    where
        is_latest = true
        and block_category = 'problem'
    group by chapter_block_id
)

, course_runs as (
    select
        course_title
        , courserun_readable_id
    from {{ ref('int__combined__course_runs') }}
    group by
        course_title
        , courserun_readable_id
)

, subsection_blocks as (
    select * from {{ ref('dim_course_content') }}
    where is_latest = true
)

, section_blocks as (
    select * from {{ ref('dim_course_content') }}
    where is_latest = true
)

, problem_blocks as (
    select * from {{ ref('dim_course_content') }}
    where is_latest = true
)

select
    problem_engagement.platform
    , coalesce(dim_user.email, dim_user_edxorg.email) as user_email
    , coalesce(dim_user.full_name, dim_user_edxorg.full_name) as full_name
    , problem_engagement.courserun_readable_id
    , course_runs.course_title
    , section_blocks.block_title as section_title
    , section_blocks.block_index as section_block_index
    , subsection_blocks.block_title as subsection_title
    , subsection_blocks.block_index as subsection_block_index
    , problem_blocks.block_title as problem_title
    , problem_blocks.block_index as problem_block_index
    , problem_engagement.openedx_user_id
    , problem_engagement.problem_block_fk
    , max(problem_engagement.num_of_attempts) as num_of_attempts
    , max(problem_engagement.num_of_correct_attempts) as num_of_correct_attempts
    , max(problem_grades.max_possible_grade) as max_possible_grade
    , min(problem_grades.learner_lowest_grade) as learner_lowest_grade
    , max(problem_grades.learner_highest_grade) as learner_highest_grade
    , max(case when problem_engagement.num_of_correct_attempts > 0 then 1 else 0 end)
        as correct_true_cnt
    , max(case when problem_engagement.num_of_attempts > 0 then 1 else 0 end)
        as attempted_true_cnt
    , max(problems_per_chapter.number_of_problems) as number_of_problems
from problem_engagement
left join dim_user
    on problem_engagement.platform = 'mitxonline'
    and problem_engagement.openedx_user_id = dim_user.mitxonline_openedx_user_id
left join dim_user as dim_user_edxorg
    on problem_engagement.platform = 'edxorg'
    and problem_engagement.openedx_user_id = dim_user_edxorg.edxorg_openedx_user_id
inner join subsection_blocks
    on problem_engagement.sequential_block_fk = subsection_blocks.block_id
inner join section_blocks
    on problem_engagement.chapter_block_fk = section_blocks.block_id
inner join problem_blocks
    on problem_engagement.problem_block_fk = problem_blocks.block_id
inner join problem_grades
    on problem_engagement.platform = problem_grades.platform
    and problem_engagement.problem_block_fk = problem_grades.problem_block_fk
    and problem_engagement.courserun_readable_id = problem_grades.courserun_readable_id
    and problem_engagement.openedx_user_id = problem_grades.openedx_user_id
left join course_runs
    on problem_engagement.courserun_readable_id = course_runs.courserun_readable_id
left join problems_per_chapter
    on problem_engagement.chapter_block_fk = problems_per_chapter.chapter_block_fk
group by
    problem_engagement.platform
    , coalesce(dim_user.email, dim_user_edxorg.email)
    , coalesce(dim_user.full_name, dim_user_edxorg.full_name)
    , problem_engagement.courserun_readable_id
    , course_runs.course_title
    , section_blocks.block_title
    , section_blocks.block_index
    , subsection_blocks.block_title
    , subsection_blocks.block_index
    , problem_blocks.block_title
    , problem_blocks.block_index
    , problem_engagement.openedx_user_id
    , problem_engagement.problem_block_fk
