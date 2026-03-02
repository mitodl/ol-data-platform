---MITx users from MITx Online and edX with dedup
---For users exist on both MITx Online and edX.org, profile data are COALESCE from MITx Online, MicroMasters or edX.org

with users as (
    select * from {{ ref('int__mitxonline__users') }}
)
----deduplicate users based on their openedx user_username, prioritizing those with an openedx_user_id
-- and the most recent joined date

, mitxonline_users as (
    select * from (
        select
            *
            , row_number() over (
                partition by user_username
                order by openedx_user_id asc nulls last, user_joined_on desc
            ) as row_num
        from users
    )
    where row_num = 1
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
        , mitxonline_users.user_global_id
        , mitxonline_users.user_username as user_mitxonline_username
        , mitxonline_users.user_edxorg_username
        , mitxonline_users.user_email as user_mitxonline_email
        , micromasters_users.user_email as user_micromasters_email
        , micromasters_users.user_id as user_micromasters_id
        , mitxonline_users.user_full_name
        , mitxonline_users.user_first_name
        , mitxonline_users.user_last_name
        , mitxonline_users.user_joined_on
        , mitxonline_users.user_last_login
        , mitxonline_users.user_is_active
        , true as is_mitxonline_user
        , coalesce(mitxonline_users.user_company, micromasters_users.user_company_name) as user_company
        , coalesce(mitxonline_users.user_industry, micromasters_users.user_company_industry) as user_industry
        , coalesce(mitxonline_users.user_job_title, micromasters_users.user_job_position) as user_job_title
        , coalesce(
            mitxonline_users.user_address_country, micromasters_users.user_address_country
        ) as user_address_country
        , coalesce(
            mitxonline_users.user_address_state, micromasters_users.user_address_state_or_territory
        ) as user_address_state
        , coalesce(
            mitxonline_users.user_highest_education, micromasters_users.user_highest_education
        ) as user_highest_education
        , coalesce(mitxonline_users.user_gender, micromasters_users.user_gender) as user_gender
        , coalesce(
            mitxonline_users.user_birth_year, cast(substring(micromasters_users.user_birth_date, 1, 4) as int)
        ) as user_birth_year
    from mitxonline_users
    left join micromasters_users on mitxonline_users.user_username = micromasters_users.user_mitxonline_username
)

---augment edxorg users profile with MicroMasters
, edxorg_users_view as (
    select
        edxorg_users.user_id as user_edxorg_id
        , edxorg_users.user_username as user_edxorg_username
        , edxorg_users.user_email as user_edxorg_email
        , micromasters_users.user_email as user_micromasters_email
        , micromasters_users.user_id as user_micromasters_id
        , micromasters_users.user_first_name
        , micromasters_users.user_last_name
        , edxorg_users.user_joined_on
        , edxorg_users.user_last_login
        , edxorg_users.user_is_active
        , micromasters_users.user_company_name
        , micromasters_users.user_company_industry
        , micromasters_users.user_job_position
        , micromasters_users.user_address_state_or_territory
        , micromasters_users.user_address_postal_code
        , micromasters_users.user_street_address
        , micromasters_users.user_address_city
        , true as is_edxorg_user
        , coalesce(micromasters_users.user_full_name, edxorg_users.user_full_name) as user_full_name
        , coalesce(micromasters_users.user_address_country, edxorg_users.user_country) as user_address_country
        , coalesce(
            micromasters_users.user_highest_education, edxorg_users.user_highest_education
        ) as user_highest_education
        , coalesce(micromasters_users.user_gender, edxorg_users.user_gender) as user_gender
        , coalesce(
            cast(substring(micromasters_users.user_birth_date, 1, 4) as int), edxorg_users.user_birth_year
        ) as user_birth_year
    from edxorg_users
    left join micromasters_users on edxorg_users.user_username = micromasters_users.user_edxorg_username
)

, mitxonline_edxorg_users as (
    select
        mitxonline_users_view.user_mitxonline_id
        , edxorg_users_view.user_edxorg_id
        , mitxonline_users_view.user_global_id
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
        , edxorg_users_view.user_address_postal_code
        , edxorg_users_view.user_street_address
        , edxorg_users_view.user_address_city
        , coalesce(mitxonline_users_view.is_mitxonline_user is not null, false) as is_mitxonline_user
        , coalesce(edxorg_users_view.is_edxorg_user is not null, false) as is_edxorg_user
        , coalesce(mitxonline_users_view.user_full_name, edxorg_users_view.user_full_name) as user_full_name
        , coalesce(mitxonline_users_view.user_first_name, edxorg_users_view.user_first_name) as user_first_name
        , coalesce(mitxonline_users_view.user_last_name, edxorg_users_view.user_last_name) as user_last_name
        , coalesce(mitxonline_users_view.user_address_country, edxorg_users_view.user_address_country)
            as user_address_country
        , coalesce(mitxonline_users_view.user_address_state, edxorg_users_view.user_address_state_or_territory)
            as user_address_state
        , coalesce(mitxonline_users_view.user_highest_education, edxorg_users_view.user_highest_education)
            as user_highest_education
        , coalesce(mitxonline_users_view.user_gender, edxorg_users_view.user_gender) as user_gender
        , coalesce(mitxonline_users_view.user_birth_year, edxorg_users_view.user_birth_year) as user_birth_year
        , coalesce(mitxonline_users_view.user_company, edxorg_users_view.user_company_name) as user_company
        , coalesce(mitxonline_users_view.user_job_title, edxorg_users_view.user_job_position) as user_job_title
        , coalesce(mitxonline_users_view.user_industry, edxorg_users_view.user_company_industry) as user_industry
        , coalesce(
            mitxonline_users_view.user_micromasters_id, edxorg_users_view.user_micromasters_id
        ) as user_micromasters_id
        , coalesce(
            mitxonline_users_view.user_micromasters_email, edxorg_users_view.user_micromasters_email
        ) as user_micromasters_email
    from mitxonline_users_view
    full outer join edxorg_users_view
        on mitxonline_users_view.user_edxorg_username = edxorg_users_view.user_edxorg_username
)

select
    is_mitxonline_user
    , is_edxorg_user
    , user_global_id
    , user_mitxonline_id
    , user_edxorg_id
    , user_micromasters_id
    , user_mitxonline_username
    , user_edxorg_username
    , user_mitxonline_email
    , user_edxorg_email
    , user_micromasters_email
    , user_joined_on_mitxonline
    , user_joined_on_edxorg
    , user_last_login_on_mitxonline
    , user_last_login_on_edxorg
    , user_is_active_on_mitxonline
    , user_is_active_on_edxorg
    , user_full_name
    , user_first_name
    , user_last_name
    , user_address_country
    , user_address_state
    , user_address_city
    , user_address_postal_code
    , user_street_address
    , user_highest_education
    , user_gender
    , user_birth_year
    , user_company
    , user_industry
    , user_job_title
    , case
        when is_mitxonline_user = true
            then {{ generate_hash_id("cast(user_mitxonline_id as varchar) || 'MITx Online'") }}
        when is_edxorg_user = true
            then {{ generate_hash_id("cast(user_edxorg_id as varchar) || 'edX.org'") }}
    end as user_hashed_id
from mitxonline_edxorg_users

union distinct
--- append micromasters users who don't exist in mitxonline_edxorg_users
select
    if(micromasters_users.user_mitxonline_username is not null, true, false) as is_mitxonline_user
    , if(micromasters_users.user_edxorg_username is not null, true, false) as is_edxorg_user
    , null as user_global_id
    , null as user_mitxonline_id
    , null as user_edxorg_id
    , micromasters_users.user_id as user_micromasters_id
    , micromasters_users.user_mitxonline_username
    , micromasters_users.user_edxorg_username
    , null as user_mitxonline_email
    , null as user_edxorg_email
    , micromasters_users.user_email as user_micromasters_email
    , null as user_joined_on_mitxonline
    , null as user_joined_on_edxorg
    , null as user_last_login_on_mitxonline
    , null as user_last_login_on_edxorg
    , null as user_is_active_on_mitxonline
    , null as user_is_active_on_edxorg
    , micromasters_users.user_full_name
    , micromasters_users.user_first_name
    , micromasters_users.user_last_name
    , micromasters_users.user_address_country
    , micromasters_users.user_address_state_or_territory as user_address_state
    , micromasters_users.user_address_city
    , micromasters_users.user_address_postal_code
    , micromasters_users.user_street_address
    , micromasters_users.user_highest_education
    , micromasters_users.user_gender
    , cast(substring(micromasters_users.user_birth_date, 1, 4) as int) as user_birth_year
    , micromasters_users.user_company_name as user_company
    , micromasters_users.user_company_industry as user_industry
    , micromasters_users.user_job_position as user_job_title
    , {{ generate_hash_id("cast(user_micromasters_id as varchar) || 'MicroMasters'") }} as user_hashed_id
from micromasters_users
left join mitxonline_edxorg_users
    on micromasters_users.user_id = mitxonline_edxorg_users.user_micromasters_id
where mitxonline_edxorg_users.user_micromasters_id is null
