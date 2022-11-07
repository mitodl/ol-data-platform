-- Program Certificate information for MITx Online

with certificates as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_programcertificate') }}
)

, programs as (
    select * from {{ ref('stg__mitxonline__app__postgres__courses_program') }}
)

, users as (
    select * from {{ ref('stg__mitxonline__app__postgres__users_user') }}
)

select
    certificates.programcertificate_id
    , certificates.programcertificate_uuid
    , certificates.program_id
    , programs.program_title
    , programs.program_readable_id
    , certificates.programcertificate_is_revoked
    , certificates.user_id
    , users.user_username
    , users.user_email
from certificates
inner join programs on certificates.program_id = programs.program_id
inner join users on certificates.user_id = users.user_id
