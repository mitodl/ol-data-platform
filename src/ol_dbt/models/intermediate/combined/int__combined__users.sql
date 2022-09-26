with mitxonline_users as (
    select 
        id,
        username,
        user_email as email
    from {{ ref('int__mitxonline__users') }}
)

, mitxpro_users as (
    select * from {{ ref('int__mitxpro__users') }}
)

, bootcamps_users as (
    select * from {{ ref('int__bootcamps__users') }}
)

, join_ol_users as (
    select
        *
        , 'MITx Online' as platform
    from mitxonline_users

    union all

    select
        *
        , 'xPro' as platform
    from mitxpro_users

    union all

    select
        *
        , 'Bootcamps' as platform
    from bootcamps_users
)

select * from join_ol_users
