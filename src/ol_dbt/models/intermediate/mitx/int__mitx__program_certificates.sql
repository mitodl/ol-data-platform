{{ config(materialized='view') }}

with micromasters_program_certificates as (
    select * from {{ ref('int__micromasters__program_certificates') }}
)

, mitxonline_program_certificates as (
    select * from {{ ref('int__mitxonline__program_certificates') }}
    --- dedp is handled in micromasters_program_certificates
    where program_is_dedp = false
)

, mitx_programs as (
    select * from {{ ref('int__mitx__programs') }}
)

, mitx_program_certificates as (
    select
        micromasters_program_certificates.program_title
        , micromasters_program_certificates.micromasters_program_id
        , micromasters_program_certificates.mitxonline_program_id
        , micromasters_program_certificates.program_completion_timestamp
        , micromasters_program_certificates.user_mitxonline_username
        , micromasters_program_certificates.user_edxorg_username
        , micromasters_program_certificates.user_email
        , micromasters_program_certificates.user_full_name
    from micromasters_program_certificates

    union all

    select
        mitxonline_program_certificates.program_title
        , mitx_programs.micromasters_program_id
        , mitxonline_program_certificates.program_id as mitxonline_program_id
        , mitxonline_program_certificates.programcertificate_created_on as program_completion_timestamp
        , mitxonline_program_certificates.user_username as user_mitxonline_username
        , mitxonline_program_certificates.user_edxorg_username
        , mitxonline_program_certificates.user_email
        , mitxonline_program_certificates.user_full_name
    from mitxonline_program_certificates
    left join mitx_programs
        on mitxonline_program_certificates.program_id = mitx_programs.mitxonline_program_id
)

select * from mitx_program_certificates
