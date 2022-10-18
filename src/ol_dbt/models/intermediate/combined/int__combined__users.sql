with mitxonline_users as (
    select
        user_id
        , user_username
        , user_email
    from {{ ref('int__mitxonline__users') }}
)

, mitxpro_users as (
    select
        user_id
        , user_username
        , user_email
    from {{ ref('int__mitxpro__users') }}
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
)

select * from join_ol_users
