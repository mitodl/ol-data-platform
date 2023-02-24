with program_certificates_dedp_from_micromasters as (
    select *
    from {{ ref('mart_components__micromasters_program_certificates_dedp_from_micromasters') }}
)

, program_certificates_dedp_from_mitxonline as (
    select *
    from {{ ref('mart_components__micromasters_program_certificates_dedp_from_mitxonline') }}
)

, program_certificates_non_dedp as (
    select *
    from {{ ref('mart_components__micromasters_program_certificates_non_dedp') }}
)

-- Some micromasters learners from previous semesters don't have mitxonline logins. The mitxonline
-- database does not include the certificates for those users. However, for future semesters,
-- the mitxonline  database will be the source for dedp program certificates and the table in
-- the micromasters database will no longer be updated. Hence we query the micromasters database for
-- dedp certificates earned before 2022-10-01 and the mitxonline database for those earned after

, report as (
    select *
    from program_certificates_dedp_from_micromasters
    where program_completion_timestamp < '2022-10-01'

    union all

    select *
    from program_certificates_dedp_from_mitxonline
    where program_completion_timestamp >= '2022-10-01'

    union all

    select * from program_certificates_non_dedp
)


select
    user_edxorg_username
    , user_email
    , program_title
    , user_edxorg_id
    , program_completion_timestamp
    , user_gender
    , user_address_city
    , user_first_name
    , user_last_name
    , user_full_name
    , user_year_of_birth
    , user_country
    , user_address_postal_code
    , user_street_address
    , user_address_state_or_territory
from report
order by program_completion_timestamp desc
