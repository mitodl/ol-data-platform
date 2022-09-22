with mitxonline_course_runs as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

, mitxpro_course_runs as (
    select * from {{ ref('int__mitxpro__course_runs') }}
)

, bootcamps_course_runs as (
    select * from {{ ref('int__bootcamps__course_runs') }}
)

, join_ol_course_runs as (
    select
        *
        , 'MITx Online' as platform
    from mitxonline_course_runs

    union all

    select
        *
        , 'xPro' as platform
    from mitxpro_course_runs

    union all

    select
        *
        , 'Bootcamps' as platform
    from bootcamps_course_runs
)

select * from join_ol_course_runs
