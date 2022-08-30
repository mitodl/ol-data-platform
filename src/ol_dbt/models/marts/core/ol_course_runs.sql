with mitxonline_course_runs as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

, join_ol_course_runs as (
    select
        *
        , 'MITx Online' as platform
    from mitxonline_course_runs
)

select * from join_ol_course_runs
