with program_certificates as (
    select * from {{ ref('stg__micromasters__app__postgres__grades_programcertificate') }}
)

select
    programcertificate_id
    , programcertificate_created_on
    , programcertificate_updated_on
    , programcertificate_hash
    , program_id
    , user_id
from program_certificates
