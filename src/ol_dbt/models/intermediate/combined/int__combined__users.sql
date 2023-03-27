--- This model combines intermediate users from different platform,
-- it's built as view with no additional data is stored
{{ config(materialized='view') }}

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
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
    from mitxonline_users

    union all

    select
        '{{ var("mitxpro") }}' as platform
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
        '{{ var("bootcamps") }}' as platform
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
        '{{ var("edxorg") }}' as platform
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
