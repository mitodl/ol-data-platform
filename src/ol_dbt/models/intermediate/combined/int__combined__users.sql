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

, emeritus_users as (
    select
        user_id
        , user_email
        , user_full_name
        , user_address_country
        , user_gender
        , user_company
        , user_job_title
        , user_industry
        , user_address_street
        , user_address_city
        , user_address_state
        , user_address_postal_code
        , row_number() over (
            partition by coalesce(user_id, user_email, user_full_name)
            order by user_gdpr_consent_date desc, enrollment_created_on desc
        ) as row_num
    from {{ ref('stg__emeritus__api__bigquery__user_enrollments') }}
)

, global_alumni_users as (
    select
        user_id
        , user_email
        , user_full_name
        , user_address_country
        , user_gender
        , user_company
        , user_job_title
        , user_industry
        , user_address_street
        , user_address_city
        , user_address_state
        , user_address_postal_code
        , row_number() over (
            partition by user_email
            order by user_gdpr_consent_date desc, courserun_start_on desc
        ) as row_num
    from {{ ref('stg__global_alumni__api__bigquery__user_enrollments') }}
)

, combined_users as (
    select
        '{{ var("mitxonline") }}' as platform
        , cast(user_id as varchar) as user_id
        , openedx_user_id
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
        , user_street_address
        , user_address_city
        , user_address_state as user_address_state_or_territory
        , user_address_postal_code
    from mitxonline_users

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , cast(user_id as varchar) as user_id
        , openedx_user_id
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
        , user_street_address
        , user_address_city
        , user_address_state_or_territory
        , user_address_postal_code
    from mitxpro_users

    union all

    select
        '{{ var("emeritus") }}' as platform
        , user_id
        , null as openedx_user_id
        , null as user_username
        , user_email
        , user_full_name
        , user_address_country
        , null as user_highest_education
        , user_gender
        , null as user_birth_year
        , user_company
        , user_job_title
        , user_industry
        , null as user_joined_on
        , null as user_last_login
        , null as user_is_active
        , user_address_street as user_street_address
        , user_address_city
        , user_address_state as user_address_state_or_territory
        , user_address_postal_code
    from emeritus_users
    where row_num = 1

    union all

    select
        '{{ var("global_alumni") }}' as platform
        , user_id
        , null as openedx_user_id
        , null as user_username
        , user_email
        , user_full_name
        , user_address_country
        , null as user_highest_education
        , user_gender
        , null as user_birth_year
        , user_company
        , user_job_title
        , user_industry
        , null as user_joined_on
        , null as user_last_login
        , null as user_is_active
        , user_address_street as user_street_address
        , user_address_city
        , user_address_state as user_address_state_or_territory
        , user_address_postal_code
    from global_alumni_users
    where row_num = 1

    union all

    select
        '{{ var("bootcamps") }}' as platform
        , cast(user_id as varchar) as user_id
        , null as openedx_user_id
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
        , user_street_address
        , user_address_city
        , user_address_state_or_territory
        , user_address_postal_code
    from bootcamps_users

    union all

    select
        '{{ var("edxorg") }}' as platform
        , cast(edxorg_users.user_id as varchar) as user_id
        , edxorg_users.user_id as openedx_user_id
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
        , micromasters_users.user_street_address
        , micromasters_users.user_address_city
        , micromasters_users.user_address_state_or_territory
        , micromasters_users.user_address_postal_code
    from edxorg_users
    left join micromasters_users on edxorg_users.user_username = micromasters_users.user_edxorg_username

    union all

    select
        '{{ var("residential") }}' as platform
        , cast(user_id as varchar) as user_id
        , user_id as openedx_user_id
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
        , user_street_address
        , user_address_city
        , user_address_state_or_territory
        , user_address_postal_code
    from residential_users
)

select
    case
        when user_id is not null
            then {{ generate_hash_id('user_id || platform') }}
        when user_email is not null
            then {{ generate_hash_id('user_email || platform') }}
        else
            {{ generate_hash_id('user_full_name || platform') }}
    end as user_hashed_id
    , *
from combined_users
