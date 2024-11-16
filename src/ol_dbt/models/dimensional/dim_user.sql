with mitxonline_users as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

, mitxonline_legaladdress as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_legaladdress') }}
)

, mitxonline_profile as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__users_userprofile') }}
)

, mitxonline_user_view as (
    select
        mitxonline_users.user_username
        , mitxonline_users.user_email
        , mitxonline_users.user_full_name
        , mitxonline_legaladdress.user_address_country
        , mitxonline_profile.user_highest_education
        , mitxonline_profile.user_gender
        , mitxonline_profile.user_birth_year
        , mitxonline_profile.user_company
        , mitxonline_profile.user_job_title
        , mitxonline_profile.user_industry
        , mitxonline_users.user_is_active
        , 'mitxonline' as platform
    from mitxonline_users
    left join mitxonline_legaladdress on mitxonline_users.user_id = mitxonline_legaladdress.user_id
    left join mitxonline_profile on mitxonline_users.user_id = mitxonline_profile.user_id
)

, edx_user as (
    select *
    from {{ ref('stg__edxorg__bigquery__mitx_user_info_combo') }}
)

, edx_usersocialauth as (
    select *
    from {{ ref('stg__micromasters__app__postgres__auth_usersocialauth') }}
)

, edx_profile as (
    select *
    from {{ ref('stg__micromasters__app__postgres__profiles_profile') }}
)

, edx_user_employment as (
    select *
    from {{ ref('stg__micromasters__app__postgres__profiles_employment') }}
)

, mitx_user_info_combo as (
    select
        user_id
        , user_username
        , courserun_platform
        , row_number() over (partition by user_id order by user_last_login desc) as rn
    from edx_user
)

, edx_employment as (
    select
        user_profile_id
        , user_company_name
        , user_company_industry
        , row_number() over (partition by user_profile_id order by user_start_date desc) as rn
    from edx_user_employment
)

, edxorg_view as (
    select
        mitx_user_info_combo.user_username
        , mitx_user_info_combo.user_email
        , edx_profile.user_full_name
        , edx_profile.user_address_country
        , edx_profile.user_highest_education
        , edx_profile.user_gender
        , edx_profile.user_birth_date
        , edx_employment.user_company_name
        , edx_profile.user_job_title
        , edx_employment.user_company_industry
        , 'edx' as platform
    from mitx_user_info_combo
    left join edx_usersocialauth
        on
            mitx_user_info_combo.user_username = edx_usersocialauth.user_username
            and
            edx_usersocialauth.user_auth_provider = 'edxorg'
    left join edx_profile on edx_usersocialauth.user_id = edx_profile.user_id
    left join edx_employment
        on
            edx_profile.user_profile_id = edx_employment.user_profile_id
            and
            edx_employment.rn = 1
    where
        mitx_user_info_combo.rn = 1
        and
        mitx_user_info_combo.courserun_platform = 'edX.org'
)

, xpro_users as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__users_user') }}
)

, xpro_users_profile as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__users_profile') }}
)

, xpro_users_legaladdress as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__users_legaladdress') }}
)

, xpro_user_view as (
    select
        xpro_users.user_username
        , xpro_users.user_email
        , xpro_users.user_full_name
        , xpro_users_legaladdress.user_address_country
        , xpro_users_profile.user_highest_education
        , xpro_users_profile.user_gender
        , xpro_users_profile.user_birth_year
        , xpro_users_profile.user_company
        , xpro_users_profile.user_job_title
        , xpro_users_profile.user_industry
        , xpro_users.user_is_active
        , 'xpro' as platform
    from xpro_users
    left join xpro_users_profile on xpro_users.user_id = xpro_users_profile.user_id
    left join xpro_users_legaladdress on xpro_users.user_id = xpro_users_legaladdress.user_id
)

, users as (
    select *
    from {{ ref('stg__bootcamps__app__postgres__auth_user') }}
)

, users_legaladdress as (
    select *
    from {{ ref('stg__bootcamps__app__postgres__profiles_legaladdress') }}
)

, users_profile as (
    select *
    from {{ ref('stg__bootcamps__app__postgres__profiles_profile') }}
)

, bootcamps_user_view as (
    select
        users.user_username
        , users.user_email
        , users_profile.user_full_name
        , users_legaladdress.user_address_country
        , users_profile.user_highest_education
        , users_profile.user_gender
        , users_profile.user_birth_year
        , users_profile.user_company
        , users_profile.user_job_title
        , users_profile.user_industry
        , users.user_is_active
        , 'bootcamps' as platform
    from users
    left join users_legaladdress on users.user_id = users_legaladdress.user_id
    left join users_profile on users.user_id = users_profile.user_id
)

select
    {{ generate_hash_id('user_username || platform') }} as dim_user_id
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
    , user_is_active
from mitxonline_user_view

union all

select
    {{ generate_hash_id('user_username || platform') }} as dim_user_id
    , user_username
    , user_email
    , user_full_name
    , user_address_country
    , user_highest_education
    , user_gender
    , user_birth_year
    , user_company_name as user_company
    , user_job_title
    , user_company_industry as user_industry
    , null as user_is_active
from edxorg_view

union all

select
    {{ generate_hash_id('user_username || platform') }} as dim_user_id
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
    , user_is_active
from xpro_user_view

union all

select
    {{ generate_hash_id('user_username || platform') }} as dim_user_id
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
    , user_is_active
from bootcamps_user_view
