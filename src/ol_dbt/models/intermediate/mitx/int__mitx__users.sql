---MITx users from MITx Online and edX with dedup
-- For users exist on both MITx Online and edX.org, profile data are COALESCE from both sources
-- in order of MITx Online and then edX.org

{{ config(materialized='view') }}

with mitxonline_users as (
    select * from {{ ref('int__mitxonline__users') }}
)

, edxorg_users as (
    select * from {{ ref('int__edxorg__mitx_users') }}
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
        , true as is_edxorg_user
    from edxorg_users
)

, mitx_users_combined as (
    select
        mitxonline_users_view.user_mitxonline_id
        , edxorg_users_view.user_edxorg_id
        , mitxonline_users_view.user_mitxonline_username
        , edxorg_users_view.user_edxorg_username
        , mitxonline_users_view.user_mitxonline_email
        , edxorg_users_view.user_edxorg_email
        , coalesce(mitxonline_users_view.is_mitxonline_user is not null, false) as is_mitxonline_user
        , coalesce(edxorg_users_view.is_edxorg_user is not null, false) as is_edxorg_user
        , coalesce(mitxonline_users_view.user_full_name, edxorg_users_view.user_full_name) as user_full_name
        , coalesce(mitxonline_users_view.user_address_country, edxorg_users_view.user_address_country)
            as user_address_country
        , coalesce(mitxonline_users_view.user_highest_education, edxorg_users_view.user_highest_education)
            as user_highest_education
        , coalesce(mitxonline_users_view.user_gender, edxorg_users_view.user_gender) as user_gender
        , coalesce(mitxonline_users_view.user_birth_year, edxorg_users_view.user_birth_year) as user_birth_year
    from mitxonline_users_view
    full outer join edxorg_users_view
        on mitxonline_users_view.user_edxorg_username = edxorg_users_view.user_edxorg_username
)

select * from mitx_users_combined
