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

-- Guard against the dim_course_run SCD2 expiration gap: more than one is_current = true
-- row can exist for the same course run, which would fan out the course_title lookup
-- below. Keep the latest. Same guard as dim_product.sql.
, d_course_run as (
    select
        courserun_readable_id
        , course_fk
    from (
        select
            courserun_readable_id
            , course_fk
            , row_number() over (
                partition by courserun_readable_id
                order by effective_date desc nulls last
            ) as _row_num
        from {{ ref('dim_course_run') }}
        where
            is_current = true
            -- The staging pair this replaced read the MITx Online course tables only, so
            -- course_title has always been null for every other platform's course run in
            -- this report. Restricting to mitxonline preserves that; dropping the filter
            -- would newly populate 1,865 more course runs (1,321 mitxpro, 492 edxorg,
            -- 52 bootcamps), which is a data change rather than a layer migration.
            -- Note dim_course_run.platform uses the bare 'mitxonline' platform code,
            -- not var('mitxonline'), which is the display name "MITx Online".
            and platform = 'mitxonline'
    )
    where _row_num = 1
)

-- dim_course is SCD2 with the same expiration-gap shape as dim_course_run, so it gets
-- the same guard. Both are zero-fan-out in production today (0 course_pk and 0 mitxonline
-- courserun_readable_id have more than one is_current = true row), but the lookup below
-- joins without a group by and so depends on each side yielding at most one row.
, d_course as (
    select
        course_pk
        , course_title
    from (
        select
            course_pk
            , course_title
            , row_number() over (
                partition by course_pk
                order by effective_date desc nulls last
            ) as _row_num
        from {{ ref('dim_course') }}
        where is_current = true
    )
    where _row_num = 1
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

-- The staging version inner-joined courserun to course on course_id and grouped to
-- dedupe. course_id is the primary key of courses_course, so that always yielded exactly
-- one course_title per course run; the _row_num guard above is what preserves that bound
-- here, so no group by is needed.
, course_to_courserun_ref as (
    select
        d_course.course_title
        , d_course_run.courserun_readable_id
    from d_course_run
    inner join d_course
        on d_course_run.course_fk = d_course.course_pk
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
