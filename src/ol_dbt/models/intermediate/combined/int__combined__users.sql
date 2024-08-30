--- This model combines intermediate users from different platforms, it contains duplicates for
--  MITx Online and edX.org users, deduplication is handled in int__mitx__users
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

, residential_users as (
    select * from {{ ref('int__mitxresidential__users') }}
)

, micromasters_users as (
    select * from {{ ref('int__micromasters__users') }}
)

, combined_users as (
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
        , user_company
        , user_job_title
        , user_industry
        , user_joined_on
        , user_last_login
        , user_is_active
    from mitxonline_users

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , user_id
        , user_username
        , user_email
        , user_full_name
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , user_company
        , user_job_title
        , user_industry
        , user_joined_on
        , user_last_login
        , user_is_active
    from mitxpro_users

    union all

    select
        '{{ var("bootcamps") }}' as platform
        , user_id
        , user_username
        , user_email
        , user_full_name
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , user_company
        , user_job_title
        , user_industry
        , user_joined_on
        , user_last_login
        , user_is_active
    from bootcamps_users

    union all

    select
        '{{ var("edxorg") }}' as platform
        , edxorg_users.user_id
        , edxorg_users.user_username
        , edxorg_users.user_email
        , edxorg_users.user_full_name
        , edxorg_users.user_country as user_address_country
        , edxorg_users.user_highest_education
        , edxorg_users.user_gender
        , edxorg_users.user_birth_year
        , micromasters_users.user_company_name as user_company
        , micromasters_users.user_job_position as user_job_title
        , micromasters_users.user_company_industry as user_industry
        , edxorg_users.user_joined_on
        , edxorg_users.user_last_login
        , edxorg_users.user_is_active
    from edxorg_users
    left join micromasters_users on edxorg_users.user_username = micromasters_users.user_edxorg_username

    union all

    select
        '{{ var("residential") }}' as platform
        , user_id
        , user_username
        , user_email
        , user_full_name
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , null as user_company
        , null as user_job_title
        , null as user_industry
        , user_joined_on
        , user_last_login
        , user_is_active
    from residential_users
)

select * from combined_users
