with program_certificates as (
    select *
    from {{ ref('int__micromasters__program_certificates') }}
)

select
    program_certificate_hashed_id
    , user_edxorg_username
    , user_mitxonline_username
    , user_email
    , program_title
    , micromasters_program_id
    , mitxonline_program_id
    , user_edxorg_id
    , program_completion_timestamp
    , user_gender
    , user_first_name
    , user_last_name
    , user_full_name
    , user_year_of_birth
    , user_country
    , user_address_postal_code
    , user_address_city
    , user_street_address
    , user_address_state_or_territory
    , program_is_dedp
    , program_readable_id
from program_certificates
