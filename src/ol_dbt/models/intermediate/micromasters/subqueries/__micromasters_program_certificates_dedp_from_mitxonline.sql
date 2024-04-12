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


, mitx_programs as (
    select *
    from {{ ref('int__mitx__programs') }}
)


, mitxonline_users as (
    select *
    from {{ ref('int__mitxonline__users') }}
)

select
    micromasters_users.user_edxorg_username
    , mitxonline_users.user_username as user_mitxonline_username
    , mitxonline_users.user_email
    , mitx_programs.micromasters_program_id
    , mitx_programs.program_title
    , mitx_programs.mitxonline_program_id
    , edx_users.user_id as user_edxorg_id
    , micromasters_users.user_address_city
    , mitxonline_users.user_first_name
    , mitxonline_users.user_last_name
    , micromasters_users.user_address_postal_code
    , micromasters_users.user_street_address
    , {{ generate_hash_id('mitxonline_program_certificates.programcertificate_uuid') }} as program_certificate_hashed_id
    , mitxonline_program_certificates.programcertificate_created_on as program_completion_timestamp
    , micromasters_users.user_id as micromasters_user_id
    , mitxonline_users.user_full_name
    , coalesce(mitxonline_users.user_gender, micromasters_users.user_gender) as user_gender
    , coalesce(mitxonline_users.user_address_state, micromasters_users.user_address_state_or_territory)
    as user_address_state_or_territory
    , coalesce(mitxonline_users.user_address_country, micromasters_users.user_address_country) as user_country
    , coalesce(
        cast(mitxonline_users.user_birth_year as varchar)
        , substring(micromasters_users.user_birth_date, 1, 4)
    ) as user_year_of_birth
from mitxonline_program_certificates
left join mitxonline_users on mitxonline_program_certificates.user_id = mitxonline_users.user_id
left join micromasters_users on mitxonline_users.user_micromasters_profile_id = micromasters_users.user_profile_id
left join edx_users on micromasters_users.user_edxorg_username = edx_users.user_username
left join mitx_programs on mitxonline_program_certificates.program_id = mitx_programs.mitxonline_program_id
where 
    mitx_programs.is_dedp_program = true
    and mitxonline_program_certificates.programcertificate_is_revoked = false
