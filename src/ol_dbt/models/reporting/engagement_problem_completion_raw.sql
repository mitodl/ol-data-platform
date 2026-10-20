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

, problems_in_block as (
    select
        d_course_content.chapter_block_id
        , count(d_problem.problem_block_pk) as problem_numb
    from d_problem
    inner join d_course_content
        on
            d_problem.content_block_fk = d_course_content.content_block_pk
            and d_course_content.is_latest = true
    group by d_course_content.chapter_block_id
)

-- dim_course_run and dim_course are SCD2 and can hold more than one is_current = true row per
-- key, so keep the latest. Runs with no dim_course row (all residential runs) use the run title.
, course_to_courserun_ref as (
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
    f_problem_engagement.platform
    , d_user.email as user_email
    , course_to_courserun_ref.course_title
    , f_problem_engagement.courserun_readable_id
    , d_course_content.block_title as section_title
    , d_course_content.block_index as section_block_index
    , count(distinct (case when cast(f_problem_engagement.num_of_correct_attempts as int)> 0
        then f_problem_engagement.problem_block_fk else null end)) as problems_correct
    , count(distinct (case when cast(f_problem_engagement.num_of_attempts as int)> 0
        then f_problem_engagement.problem_block_fk else null end)) as problems_attempted
    , max(problems_in_block.problem_numb) as number_of_problems
    , sum(f_problem_engagement.num_of_attempts) as number_of_total_attempts
    , cast(count(distinct (case when cast(f_problem_engagement.num_of_correct_attempts as int)> 0
        then f_problem_engagement.problem_block_fk else null end)) as decimal(30,10))
        /cast(max(problems_in_block.problem_numb) as decimal(30,10)) as percent_problems_correct
    , cast(count(distinct (case when cast(f_problem_engagement.num_of_attempts as int)> 0
        then f_problem_engagement.problem_block_fk else null end)) as decimal(30,10))
        /cast(max(problems_in_block.problem_numb) as decimal(30,10)) as percent_problems_attempted
from f_problem_engagement
left join problems_in_block
    on f_problem_engagement.chapter_block_fk = problems_in_block.chapter_block_id
inner join d_user
    on f_problem_engagement.openedx_user_id = d_user.mitxonline_openedx_user_id
left join course_to_courserun_ref
    on f_problem_engagement.courserun_readable_id = course_to_courserun_ref.courserun_readable_id
    and f_problem_engagement.platform = course_to_courserun_ref.platform
inner join d_course_content
    on
        f_problem_engagement.chapter_block_fk = d_course_content.block_id
        and d_course_content.is_latest = true
group by
    f_problem_engagement.platform
    , d_user.email
    , course_to_courserun_ref.course_title
    , f_problem_engagement.courserun_readable_id
    , d_course_content.block_title
    , d_course_content.block_index
