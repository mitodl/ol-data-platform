{{ config(materialized='view') }}

with micromasters_program_certificates as (
    select *
    from {{ ref('int__edxorg__mitx_program_certificates') }}
    where program_type = 'MicroMasters'
)

, edx_users as (
    select *
    from {{ ref('int__edxorg__mitx_users') }}
)

, micromasters_users as (
    select *
    from {{ ref('__micromasters__users') }}
)

, programs as (
    select *
    from {{ ref('int__mitx__programs') }}
    where is_dedp_program = false
)

, program_certificates_override_list as (
    select *
    from {{ ref('stg__micromasters__app__user_program_certificate_override_list') }}
)

, non_dedp_certificates as (
    select
        edx_users.user_username as user_edxorg_username
        , micromasters_users.user_mitxonline_username
        , edx_users.user_email
        , programs.micromasters_program_id
        , programs.program_title
        , programs.mitxonline_program_id
        , program_completions.user_edxorg_id
        , edx_users.user_gender
        , edx_users.user_country
        , micromasters_users.user_address_city
        , micromasters_users.user_first_name
        , micromasters_users.user_last_name
        , micromasters_users.user_address_postal_code
        , micromasters_users.user_street_address
        , micromasters_users.user_address_state_or_territory
        , edx_users.user_full_name
        , micromasters_program_certificates.program_certificate_awarded_on as program_completion_timestamp
        , micromasters_users.user_id as micromasters_user_id
        , substring(micromasters_users.user_birth_date, 1, 4) as user_year_of_birth
    from micromasters_program_certificates
    left join edx_users
        on program_completions.user_edxorg_id = edx_users.user_id
    left join micromasters_users
        on program_completions.user_edxorg_username = micromasters_users.user_edxorg_username
    left join programs
        on program_completions.program_id = programs.micromasters_program_id
)

-- Some users should recieve a certificate even though they don't fulfill the requirements according
-- to the course certificates in the edxorg database. A list of these users' user ids on edx and the micromasters
-- ids on production for the program they should earn a certificate for are stored in
-- stg__micromasters__app__postgres__courses_course. These ids won't match correctly in qa

, non_dedp_overides as (
    select
        edx_users.user_username as user_edxorg_username
        , micromasters_users.user_mitxonline_username
        , edx_users.user_email
        , programs.micromasters_program_id
        , programs.program_title
        , programs.mitxonline_program_id
        , edx_users.user_id as user_edxorg_id
        , edx_users.user_gender
        , edx_users.user_country
        , micromasters_users.user_address_city
        , micromasters_users.user_first_name
        , micromasters_users.user_last_name
        , micromasters_users.user_address_postal_code
        , micromasters_users.user_street_address
        , micromasters_users.user_address_state_or_territory
        , edx_users.user_full_name
        , null as program_completion_timestamp
        , micromasters_users.user_id as micromasters_user_id
        , substring(micromasters_users.user_birth_date, 1, 4) as user_year_of_birth
    from program_certificates_override_list
    inner join edx_users
        on program_certificates_override_list.user_edxorg_id = edx_users.user_id
    left join micromasters_users
        on edx_users.user_username = micromasters_users.user_edxorg_username
    inner join programs
        on program_certificates_override_list.micromasters_program_id = programs.micromasters_program_id
    left join non_dedp_certificates
        on
            program_certificates_override_list.user_edxorg_id
            = non_dedp_certificates.user_edxorg_id and program_certificates_override_list.micromasters_program_id
            = non_dedp_certificates.micromasters_program_id
    where non_dedp_certificates.user_edxorg_username is null
)

select *
from non_dedp_certificates

union all

select *
from non_dedp_overides
