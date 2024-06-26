{{ config(materialized='view') }}

with mm_program_certificates as (
    select *
    from {{ ref('stg__micromasters__app__postgres__grades_programcertificate') }}
)

, micromasters_users as (
    select *
    from {{ ref('__micromasters__users') }}
)


, programs as (
    select *
    from {{ ref('int__mitx__programs') }}
)

, edx_users as (
    select *
    from {{ ref('int__edxorg__mitx_users') }}
)

select
    micromasters_users.user_edxorg_username
    , micromasters_users.user_mitxonline_username
    , programs.micromasters_program_id
    , programs.program_title
    , programs.mitxonline_program_id
    , edx_users.user_id as user_edxorg_id
    , {{ generate_hash_id('mm_program_certificates.programcertificate_hash') }} as program_certificate_hashed_id
    , mm_program_certificates.programcertificate_created_on as program_completion_timestamp
    , micromasters_users.user_id as micromasters_user_id
from mm_program_certificates
left join micromasters_users on mm_program_certificates.user_id = micromasters_users.user_id
left join programs
    on mm_program_certificates.program_id = programs.micromasters_program_id
left join edx_users on micromasters_users.user_edxorg_username = edx_users.user_username
where programs.is_dedp_program = true
