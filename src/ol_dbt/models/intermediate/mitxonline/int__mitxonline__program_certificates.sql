-- Program Certificate information for MITx Online

with certificates as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_programcertificate') }}
)

, programs as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_program') }}
)

, users as (
    select * from {{ ref('int__mitxonline__users') }}
)

, program_certificates as (
    select
        certificates.programcertificate_id
        , certificates.programcertificate_uuid
        , certificates.program_id
        , programs.program_title
        , programs.program_readable_id
        , programs.program_type
        , programs.program_is_dedp
        , programs.program_is_micromasters
        , certificates.programcertificate_is_revoked
        , certificates.programcertificate_created_on
        , certificates.programcertificate_updated_on
        , certificates.user_id
        , users.user_username
        , users.user_edxorg_username
        , users.user_full_name
        , users.user_email
        , if(
            certificates.programcertificate_is_revoked = false
            , concat('https://xpro.mit.edu/certificate/program/', certificates.programcertificate_uuid)
            , null
        ) as programcertificate_url
    from certificates
    inner join programs on certificates.program_id = programs.program_id
    inner join users on certificates.user_id = users.user_id
)

select * from program_certificates
