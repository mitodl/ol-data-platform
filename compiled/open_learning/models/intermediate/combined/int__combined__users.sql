--- This model combines intermediate users from different platform,
-- it's built as view with no additional data is stored


with mitxonline_users as (
    select * from dev.main_intermediate.int__mitxonline__users
)

, mitxpro_users as (
    select * from dev.main_intermediate.int__mitxpro__users
)

, bootcamps_users as (
    select * from dev.main_intermediate.int__bootcamps__users
)

, edxorg_users as (
    select * from dev.main_intermediate.int__edxorg__mitx_users
)

, combined_users as (
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

    union all

    select
        'xPro' as platform
        , user_id
        , user_username
        , user_email
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year

    from mitxpro_users

    union all

    select
        'Bootcamps' as platform
        , user_id
        , user_username
        , user_email
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
    from bootcamps_users

    union all

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

select * from combined_users
