---MITx users from MITx Online and edX with dedup
---For users exist on both MITx Online and edX.org, profile data are COALESCE from MITx Online, MicroMasters or edX.org

{{ config(materialized='view') }}

with mitxonline_users as (
    select * from {{ ref('int__mitxonline__users') }}
)

, edxorg_users as (
    select * from {{ ref('int__edxorg__mitx_users') }}
)

, micromasters_users as (
    select * from {{ ref('int__micromasters__users') }}
)

, mitxonline_users_view as (
    select
        mitxonline_users.user_id as user_mitxonline_id
        , mitxonline_users.user_username as user_mitxonline_username
        , mitxonline_users.user_edxorg_username
        , mitxonline_users.user_email as user_mitxonline_email
        , mitxonline_users.user_full_name
        , mitxonline_users.user_address_country
        , mitxonline_users.user_highest_education
        , mitxonline_users.user_gender
        , mitxonline_users.user_birth_year
        , mitxonline_users.user_company
        , mitxonline_users.user_job_title
        , mitxonline_users.user_industry
        , mitxonline_users.user_joined_on
        , mitxonline_users.user_last_login
        , mitxonline_users.user_is_active
        , true as is_mitxonline_user
    from mitxonline_users
)

, edxorg_users_view as (
    select
        edxorg_users.user_id as user_edxorg_id
        , edxorg_users.user_username as user_edxorg_username
        , edxorg_users.user_email as user_edxorg_email
        , edxorg_users.user_full_name
        , edxorg_users.user_country as user_address_country
        , edxorg_users.user_highest_education
        , edxorg_users.user_gender
        , edxorg_users.user_birth_year
        , edxorg_users.user_joined_on
        , edxorg_users.user_last_login
        , edxorg_users.user_is_active
        , micromasters_users.user_company_name
        , micromasters_users.user_company_industry
        , micromasters_users.user_job_position
        , true as is_edxorg_user
    from edxorg_users
    left join micromasters_users on edxorg_users.user_username = micromasters_users.user_edxorg_username
)

, mitx_users_combined as (
    select
        mitxonline_users_view.user_mitxonline_id
        , edxorg_users_view.user_edxorg_id
        , mitxonline_users_view.user_mitxonline_username
        , edxorg_users_view.user_edxorg_username
        , mitxonline_users_view.user_mitxonline_email
        , edxorg_users_view.user_edxorg_email
        , mitxonline_users_view.user_joined_on as user_joined_on_mitxonline
        , edxorg_users_view.user_joined_on as user_joined_on_edxorg
        , mitxonline_users_view.user_last_login as user_last_login_on_mitxonline
        , edxorg_users_view.user_last_login as user_last_login_on_edxorg
        , mitxonline_users_view.user_is_active as user_is_active_on_mitxonline
        , edxorg_users_view.user_is_active as user_is_active_on_edxorg
        , coalesce(mitxonline_users_view.is_mitxonline_user is not null, false) as is_mitxonline_user
        , coalesce(edxorg_users_view.is_edxorg_user is not null, false) as is_edxorg_user
        , coalesce(mitxonline_users_view.user_full_name, edxorg_users_view.user_full_name) as user_full_name
        , coalesce(mitxonline_users_view.user_address_country, edxorg_users_view.user_address_country)
        as user_address_country
        , coalesce(mitxonline_users_view.user_highest_education, edxorg_users_view.user_highest_education)
        as user_highest_education
        , coalesce(mitxonline_users_view.user_gender, edxorg_users_view.user_gender) as user_gender
        , coalesce(mitxonline_users_view.user_birth_year, edxorg_users_view.user_birth_year) as user_birth_year
        , coalesce(mitxonline_users_view.user_company, edxorg_users_view.user_company_name) as user_company
        , coalesce(mitxonline_users_view.user_job_title, edxorg_users_view.user_job_position) as user_job_title
        , coalesce(mitxonline_users_view.user_industry, edxorg_users_view.user_company_industry) as user_industry
    from mitxonline_users_view
    full outer join edxorg_users_view
        on mitxonline_users_view.user_edxorg_username = edxorg_users_view.user_edxorg_username
)

select * from mitx_users_combined
