{{ config(materialized='view') }}

with mitxonline_program_certificates as (
    select *
    from {{ ref('int__mitxonline__program_certificates') }}
)

, mitx_programs as (
    select *
    from {{ ref('int__mitx__programs') }}
)

select
    mitxonline_program_certificates.user_username as user_mitxonline_username
    , mitx_programs.micromasters_program_id
    , mitx_programs.program_title
    , mitx_programs.mitxonline_program_id
    , {{ generate_hash_id('mitxonline_program_certificates.programcertificate_uuid') }} as program_certificate_hashed_id
    , mitxonline_program_certificates.programcertificate_created_on as program_completion_timestamp
from mitxonline_program_certificates
left join mitx_programs on mitxonline_program_certificates.program_id = mitx_programs.mitxonline_program_id
where
    mitx_programs.is_dedp_program = true
    and mitxonline_program_certificates.programcertificate_is_revoked = false
