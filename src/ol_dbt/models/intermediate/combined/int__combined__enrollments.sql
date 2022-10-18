with mitxonline_enrollments as (
    select * from {{ ref('int__mitxonline__enrollments') }}
)

, mitxpro_enrollments as (
    select * from {{ ref('int__mitxpro__enrollments') }}
)

, join_ol_enrollments as (
    select
        *
        , 'MITx Online' as platform
    from mitxonline_enrollments

    union all

    select
        *
        , 'xPro' as platform
    from mitxpro_enrollments
)

select * from join_ol_enrollments
