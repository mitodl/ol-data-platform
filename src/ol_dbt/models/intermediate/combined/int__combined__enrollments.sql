with mitxonline_enrollments as (
    select * from {{ ref('int__mitxonline__enrollments') }}
)

, join_ol_enrollments as (
    select
        *
        , 'MITx Online' as platform
    from mitxonline_enrollments
)

select * from join_ol_enrollments
