with enrollments as (
    select *
    from {{ ref('int__micromasters__course_enrollments') }}
)

, enrollments_by_program as (
    select
        program_title
        , count(*) as total_enrollments
        , count(distinct user_email) as unique_users
        , count(distinct user_country) as unique_countries
    from enrollments
    group by program_title
    order by program_title
)

, enrollments_total as (
    select
        'total' as program_title
        , count(*) as total_enrollments
        , count(distinct user_email) as unique_users
        , count(distinct user_country) as unique_countries
    from enrollments
)

select
    program_title
    , total_enrollments
    , unique_users
    , unique_countries
from enrollments_by_program
union all
select
    program_title
    , total_enrollments
    , unique_users
    , unique_countries
from enrollments_total
