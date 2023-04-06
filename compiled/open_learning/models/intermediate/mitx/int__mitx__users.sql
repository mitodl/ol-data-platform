---MITx users from MITx Online and edX



with mitxonline_users as (
    select * from dev.main_intermediate.int__mitxonline__users
)

, edxorg_users as (
    select * from dev.main_intermediate.int__edxorg__mitx_users
)

, mitx_users as (
    select
        'MITx Online' as platform
        , user_id
        , user_username
        , user_email
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
    from mitxonline_users

    union distinct

    select
        'edX.org' as platform
        , user_id
        , user_username
        , user_email
        , user_country as user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year

    from edxorg_users
)

select * from mitx_users
