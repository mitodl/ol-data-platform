with mitxonline_enrollments as (
    select * from {{ ref('int_mitxonline__enrollments') }}
)

, mitxpro_enrollments as (
    select * from {{ ref('int_mitxpro__enrollments') }}
)

, join_ol_enrollments as (
    select
        *
        , 'MITx Online' as org
    from mitxonline_enrollments

    union all

    select
        *
        , 'xPro' as org
    from mitxpro_enrollments
)

select * from join_ol_enrollments
