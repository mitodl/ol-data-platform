with f_problem_engagement as (
    select * from {{ ref('afact_problem_engagement') }}
)

, d_problem as (
    select * from {{ ref('dim_problem') }}
)

, d_course_content as (
    select * from {{ ref('dim_course_content') }}
)

, d_user as (
    select * from {{ ref('dim_user') }}
)

, stg_mitxonline_courserun as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_courserun') }}
)

, stg_mitxonline_course as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_course') }}
)

, problems_in_block as (
    select
        d_course_content.sequential_block_id
        , count(d_problem.problem_block_pk) as problem_numb
    from d_problem
    inner join d_course_content
        on d_problem.content_block_fk = d_course_content.content_block_pk
    group by d_course_content.sequential_block_id
)

, course_to_courserun_ref as (
    select
        stg_mitxonline_course.course_title
        , stg_mitxonline_courserun.courserun_readable_id
    from stg_mitxonline_courserun
    inner join stg_mitxonline_course
        on stg_mitxonline_courserun.course_id = stg_mitxonline_course.course_id
    group by
        stg_mitxonline_course.course_title
        , stg_mitxonline_courserun.courserun_readable_id
)

select
    d_user.email as user_email
    , course_to_courserun_ref.course_title
    , f_problem_engagement.courserun_readable_id
    , d_course_content.block_title as subsection_title
    , d_course_content.block_index as subsection_block_index
    , f_problem_engagement.chapter_block_fk
    , count(distinct
        (case when
            cast(f_problem_engagement.num_of_attempts as int)> 0
                then f_problem_engagement.problem_block_fk else null end
        )
    ) as problems_attempted
    , max(problems_in_block.problem_numb) as number_of_problems
    , cast(count(distinct
        (case when cast(f_problem_engagement.num_of_attempts as int)> 0
            then f_problem_engagement.problem_block_fk else null end)) as decimal(30,10))
        /cast(max(problems_in_block.problem_numb) as decimal(30,10)) as percent_problems_attempted
from f_problem_engagement
left join problems_in_block
    on f_problem_engagement.sequential_block_fk = problems_in_block.sequential_block_id
inner join d_user
    on f_problem_engagement.openedx_user_id = d_user.mitxonline_openedx_user_id
left join course_to_courserun_ref
    on f_problem_engagement.courserun_readable_id = course_to_courserun_ref.courserun_readable_id
inner join d_course_content
    on
        f_problem_engagement.sequential_block_fk = d_course_content.block_id
        and d_course_content.is_latest = true
group by
    d_user.email
    , course_to_courserun_ref.course_title
    , f_problem_engagement.courserun_readable_id
    , d_course_content.block_title
    , d_course_content.block_index
    , f_problem_engagement.chapter_block_fk
