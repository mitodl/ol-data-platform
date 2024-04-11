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

, mitxonline_program_certificates as (
    select *
    from {{ ref('int__mitxonline__program_certificates') }}
)

-- Some micromasters learners from previous semesters don't have mitxonline logins. The mitxonline
-- database does not include the certificates for those users. However, for future semesters,
-- the mitxonline  database will be the source for dedp program certificates and the table in
-- the micromasters database will no longer be updated. Hence we query the micromasters database for
-- dedp certificates earned before 2022-10-01 and the mitxonline database for those earned after

, report as (
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
        , user_address_city
        , user_first_name
        , user_last_name
        , user_full_name
        , user_year_of_birth
        , user_country
        , user_address_postal_code
        , user_street_address
        , user_address_state_or_territory
    from program_certificates_dedp_from_micromasters
    where program_completion_timestamp < '2022-10-01'

    union all

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
        , user_address_city
        , user_first_name
        , user_last_name
        , user_full_name
        , user_year_of_birth
        , user_country
        , user_address_postal_code
        , user_street_address
        , user_address_state_or_territory
    from program_certificates_dedp_from_mitxonline
    where program_completion_timestamp >= '2022-10-01'

    union all

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
        , user_address_city
        , user_first_name
        , user_last_name
        , user_full_name
        , user_year_of_birth
        , user_country
        , user_address_postal_code
        , user_street_address
        , user_address_state_or_territory
    from program_certificates_non_dedp
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
    , report.user_address_city
    , report.user_first_name
    , report.user_last_name
    , report.user_full_name
    , report.user_year_of_birth
    , report.user_country
    , report.user_address_postal_code
    , report.user_street_address
    , report.user_address_state_or_territory
    , if(report.mitxonline_program_id in (1, 2, 3), true, false) as program_is_dedp
from report
left join mitxonline_program_certificates
    on
        report.mitxonline_program_id = mitxonline_program_certificates.program_id
        and report.user_mitxonline_username
        = mitxonline_program_certificates.user_username
where
    mitxonline_program_certificates.programcertificate_is_revoked = false
    or mitxonline_program_certificates.programcertificate_is_revoked is null
order by report.program_completion_timestamp desc
