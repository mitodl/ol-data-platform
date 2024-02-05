--- This model combines intermediate users from different platforms

with mitx__users as (
    select * from {{ ref('int__mitx__users') }}
)

, mitxpro_users as (
    select * from {{ ref('int__mitxpro__users') }}
)

, bootcamps_users as (
    select * from {{ ref('int__bootcamps__users') }}
)

, combined__users as (
    select
        user_mitxonline_id
        , user_edxorg_id
        , null as user_mitxpro_id
        , null as user_bootcamps_id
        , user_mitxonline_username
        , user_edxorg_username
        , null as user_mitxpro_username
        , null as user_bootcamps_username
        , case
            when is_mitxonline_user = false
                then user_edxorg_email
            when is_edxorg_user = false
                then user_mitxonline_email
            when user_joined_on_mitxonline > user_joined_on_edxorg
                then user_mitxonline_email
            else coalesce(user_edxorg_email, user_mitxonline_email)
        end as user_email
        , case
            when user_joined_on_mitxonline > user_joined_on_edxorg
                then user_joined_on_edxorg
            else user_joined_on_mitxonline
        end as user_joined_on
        , case
            when user_last_login_on_mitxonline > user_last_login_on_edxorg
                then user_last_login_on_mitxonline
            else user_last_login_on_edxorg
        end as user_last_login
        , case
            when is_mitxonline_user = true and is_edxorg_user = true
                then 'mitxonline and edxorg'
            when is_mitxonline_user = true
                then 'mitxonline'
            when is_edxorg_user = true
                then 'edxorg'
        end as platforms
        , user_full_name
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , user_company
        , user_job_title
        , user_industry
    from mitx__users

    union all

    select
        null as user_mitxonline_id
        , null as user_edxorg_id
        , user_id as user_mitxpro_id
        , null as user_bootcamps_id
        , null as user_mitxonline_username
        , null as user_edxorg_username
        , user_username as user_mitxpro_username
        , null as user_bootcamps_username
        , user_email
        , user_joined_on
        , user_last_login
        , 'mitxpro' as platforms
        , user_full_name
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , user_company
        , user_job_title
        , user_industry
    from mitxpro_users

    union all

    select
        null as user_mitxonline_id
        , null as user_edxorg_id
        , null as user_mitxpro_id
        , user_id as user_bootcamps_id
        , null as user_mitxonline_username
        , null as user_edxorg_username
        , null as user_mitxpro_username
        , user_username as user_bootcamps_username
        , user_email
        , user_joined_on
        , user_last_login
        , 'bootcamps' as platforms
        , user_full_name
        , user_address_country
        , user_highest_education
        , user_gender
        , user_birth_year
        , user_company
        , user_job_title
        , user_industry
    from bootcamps_users
)

select
    user_email
    , user_joined_on
    , user_last_login
    , platforms
    , user_full_name
    , user_address_country
    , user_highest_education
    , user_gender
    , user_birth_year
    , user_company
    , user_job_title
    , user_industry
    , user_mitxonline_id
    , user_edxorg_id
    , user_mitxpro_id
    , user_bootcamps_id
    , user_mitxonline_username
    , user_edxorg_username
    , user_mitxpro_username
    , user_bootcamps_username
from combined__users
