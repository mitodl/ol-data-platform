{{ config(materialized='view') }}

with mitxonline_program_certificates as (
    select *
    from {{ ref('int__mitxonline__program_certificates') }}
)

, edx_users as (
    select *
    from {{ ref('int__edxorg__mitx_users') }}
)

, micromasters_users as (
    select *
    from {{ ref('__micromasters__users') }}
)


, micromasters_programs as (
    select *
    from {{ ref('int__micromasters__programs') }}
)

, mitxonline_users as (
    select *
    from {{ ref('int__mitxonline__users') }}
)

select
    micromasters_users.user_edxorg_username as user_edxorg_username
    , micromasters_users.user_email
    , micromasters_programs.program_id as micromasters_program_id
    , micromasters_programs.program_title
    , edx_users.user_id as user_edxorg_id
    , edx_users.user_gender
    , micromasters_users.user_address_city
    , micromasters_users.user_first_name
    , micromasters_users.user_last_name
    , micromasters_users.user_address_postal_code
    , micromasters_users.user_street_address
    , micromasters_users.user_address_state_or_territory
    , mitxonline_program_certificates.programcertificate_created_on as program_completion_timestamp
    , micromasters_users.user_id as micromasters_user_id
    , coalesce(edx_users.user_country, mitxonline_users.user_address_country) as user_country
    , coalesce(edx_users.user_full_name, mitxonline_users.user_full_name) as user_full_name
    , substring(micromasters_users.user_birth_date, 1, 4) as user_year_of_birth
from mitxonline_program_certificates
left join mitxonline_users on mitxonline_users.user_id = mitxonline_program_certificates.user_id
left join micromasters_users on mitxonline_users.user_micromasters_profile_id = micromasters_users.user_profile_id
left join micromasters_programs
    on micromasters_programs.program_title = 'Data, Economics, and Development Policy'
left join edx_users on edx_users.user_username = micromasters_users.user_edxorg_username
where mitxonline_program_certificates.program_title = 'Data, Economics and Development Policy'
