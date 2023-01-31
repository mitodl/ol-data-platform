{{ config(materialized='view') }}

with mm_program_certificates as (
    select *
    from {{ ref('int__micromasters__program_certificates') }}
)

, micromasters_users as (
    select *
    from {{ ref('int__micromasters__users') }}
)


, micromasters_profiles as (
    select *
    from {{ ref('stg__micromasters__app__postgres__profiles_profile') }}
)

, micromasters_programs as (
    select *
    from {{ ref('int__micromasters__programs') }}
)

, edx_users as (
    select *
    from {{ ref('int__edxorg__mitx_users') }}
)

select
    micromasters_users.user_edxorg_username as user_username
    , micromasters_users.user_email
    , micromasters_programs.program_id as micromasters_program_id
    , micromasters_programs.program_title
    , edx_users.user_id as user_edxorg_id
    , edx_users.user_gender
    , edx_users.user_country
    , micromasters_profiles.user_address_city
    , micromasters_profiles.user_first_name
    , micromasters_profiles.user_last_name
    , micromasters_profiles.user_address_postal_code
    , micromasters_profiles.user_street_address
    , micromasters_profiles.user_address_state_or_territory
    , edx_users.user_full_name
    , mm_program_certificates.programcertificate_created_on as program_completion_timestamp
    , micromasters_profiles.user_id as micromasters_user_id
    , substring(micromasters_profiles.user_birth_date, 1, 4) as user_year_of_birth
from mm_program_certificates
left join micromasters_users on mm_program_certificates.user_id = micromasters_users.user_id
left join micromasters_profiles
    on micromasters_profiles.user_id = micromasters_users.user_id
left join micromasters_programs
    on micromasters_programs.program_id = mm_program_certificates.program_id
left join edx_users on edx_users.user_username = micromasters_users.user_edxorg_username
where micromasters_programs.program_title = 'Data, Economics, and Development Policy'
