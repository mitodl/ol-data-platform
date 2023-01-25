with mitxonline_users as (
    select * from {{ ref('int__mitxonline__users') }}
)

, mitxpro_users as (
    select * from {{ ref('int__mitxpro__users') }}
)

, bootcamps_users as (
    select * from {{ ref('int__bootcamps__users') }}
)

, edxorg_users as (
    select * from {{ ref('int__edxorg__mitx_users') }}
)

, combined_users as (
    select
        '{{ var("mitxonline") }}' as platform
        , user_id
        , user_username
        , user_email
    from mitxonline_users

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , user_id
        , user_username
        , user_email

    from mitxpro_users

    union all

    select
        '{{ var("bootcamps") }}' as platform
        , user_id
        , user_username
        , user_email

    from bootcamps_users

    union all

    select
        '{{ var("edxorg") }}' as platform
        , user_id
        , user_username
        , user_email

    from edxorg_users
)

select * from combined_users
