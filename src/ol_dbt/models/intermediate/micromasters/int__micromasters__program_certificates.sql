with program_certificates_dedp_from_micromasters as (
    select *
    from {{ ref('__micromasters_program_certificates_dedp_from_micromasters') }}
)

, program_certificates_dedp_from_mitxonline as (
    select *
    from {{ ref('__micromasters_program_certificates_dedp_from_mitxonline') }}
)

, program_certificates_non_dedp as (
    select *
    from {{ ref('__micromasters_program_certificates_non_dedp') }}
)

, mitx_users as (
    select * from {{ ref('int__mitx__users') }}
)

-- Some micromasters learners from previous semesters don't have mitxonline logins. The mitxonline
-- database does not include the certificates for those users. However, for future semesters,
-- the mitxonline  database will be the source for dedp program certificates and the table in
-- the micromasters database will no longer be updated. Hence we query the micromasters database for
-- dedp certificates earned before 2022-10-01 and the mitxonline database for those earned after

, report as (
    select
        program_certificates_dedp_from_micromasters.program_certificate_hashed_id
        , program_certificates_dedp_from_micromasters.program_title
        , program_certificates_dedp_from_micromasters.micromasters_program_id
        , program_certificates_dedp_from_micromasters.mitxonline_program_id
        , program_certificates_dedp_from_micromasters.program_completion_timestamp
        , mitx_users.user_edxorg_id
        , mitx_users.user_edxorg_username
        , mitx_users.user_mitxonline_username
        , mitx_users.user_micromasters_email as user_email
        , mitx_users.user_first_name
        , mitx_users.user_last_name
        , mitx_users.user_full_name
        , mitx_users.user_gender
        , mitx_users.user_birth_year as user_year_of_birth
        , mitx_users.user_address_country as user_country
        , mitx_users.user_address_state as user_address_state_or_territory
        , mitx_users.user_address_city
        , mitx_users.user_address_postal_code
        , mitx_users.user_street_address
        , true as program_is_dedp
    from program_certificates_dedp_from_micromasters
    left join mitx_users
        on program_certificates_dedp_from_micromasters.user_micromasters_id = mitx_users.user_micromasters_id
    where program_certificates_dedp_from_micromasters.program_completion_timestamp < '2022-10-01'

    union all

    select
        program_certificates_dedp_from_mitxonline.program_certificate_hashed_id
        , program_certificates_dedp_from_mitxonline.program_title
        , program_certificates_dedp_from_mitxonline.micromasters_program_id
        , program_certificates_dedp_from_mitxonline.mitxonline_program_id
        , program_certificates_dedp_from_mitxonline.program_completion_timestamp
        , mitx_users.user_edxorg_id
        , mitx_users.user_edxorg_username
        , mitx_users.user_mitxonline_username
        , mitx_users.user_mitxonline_email as user_email
        , mitx_users.user_first_name
        , mitx_users.user_last_name
        , mitx_users.user_full_name
        , mitx_users.user_gender
        , mitx_users.user_birth_year as user_year_of_birth
        , mitx_users.user_address_country as user_country
        , mitx_users.user_address_state as user_address_state_or_territory
        , mitx_users.user_address_city
        , mitx_users.user_address_postal_code
        , mitx_users.user_street_address
        , true as program_is_dedp
    from program_certificates_dedp_from_mitxonline
    left join mitx_users
        on program_certificates_dedp_from_mitxonline.user_mitxonline_id = mitx_users.user_mitxonline_id
    where program_certificates_dedp_from_mitxonline.program_completion_timestamp >= '2022-10-01'

    union all

    select
        program_certificates_non_dedp.program_certificate_hashed_id
        , program_certificates_non_dedp.program_title
        , program_certificates_non_dedp.micromasters_program_id
        , program_certificates_non_dedp.mitxonline_program_id
        , program_certificates_non_dedp.program_completion_timestamp
        , mitx_users.user_edxorg_id
        , mitx_users.user_edxorg_username
        , mitx_users.user_mitxonline_username
        , mitx_users.user_edxorg_email as user_email
        , mitx_users.user_first_name
        , mitx_users.user_last_name
        , mitx_users.user_full_name
        , mitx_users.user_gender
        , mitx_users.user_birth_year as user_year_of_birth
        , mitx_users.user_address_country as user_country
        , mitx_users.user_address_state as user_address_state_or_territory
        , mitx_users.user_address_city
        , mitx_users.user_address_postal_code
        , mitx_users.user_street_address
        , false as program_is_dedp
    from program_certificates_non_dedp
    left join mitx_users
        on program_certificates_non_dedp.user_edxorg_id = mitx_users.user_edxorg_id
)


select
    report.program_certificate_hashed_id
    , report.user_edxorg_username
    , report.user_mitxonline_username
    , report.user_email
    , report.program_title
    , report.micromasters_program_id
    , report.mitxonline_program_id
    , report.user_edxorg_id
    , report.program_completion_timestamp
    , report.user_gender
    , report.user_first_name
    , report.user_last_name
    , report.user_full_name
    , report.user_year_of_birth
    , report.user_country
    , report.user_address_postal_code
    , report.user_address_city
    , report.user_street_address
    , report.user_address_state_or_territory
    , report.program_is_dedp
from report
order by report.program_completion_timestamp desc
