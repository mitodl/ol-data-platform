---MITx users from MITx Online and edX

{{ config(materialized='view') }}

with mitxonline_users as (
    select * from {{ ref('int__mitxonline__users') }}
)

, edxorg_users as (
    select * from {{ ref('int__edxorg__mitx_users') }}
)

, mitx_users as (
    select
        '{{ var("mitxonline") }}' as platform
        , user_id
        , user_username
        , user_email
        , user_full_name
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
    from mitxonline_users

    union distinct

    select
        '{{ var("edxorg") }}' as platform
        , user_id
        , user_username
        , user_email
        , user_full_name
        , user_country as user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year

    from edxorg_users
)

select * from mitx_users
