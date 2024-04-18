with micromasters_enrollments as (
    select *
    from {{ ref('int__micromasters__course_enrollments') }}
)

, micromasters_course_certificates as (
    select *
    from {{ ref('int__micromasters__course_certificates') }}
)

, micromasters_program_certificates as (
    select *
    from {{ ref('int__micromasters__program_certificates') }}
)

, mitx_programs as (
    select *
    from {{ ref('int__mitx__programs') }}
)

, enrollments as (
    select
        case when
            mitx_programs.is_dedp_program = true then 'Data, Economics, and Design of Policy'
        else micromasters_enrollments.program_title end as program_title
        , count(
            distinct micromasters_enrollments.courserun_readable_id
            || micromasters_enrollments.user_email
        ) as total_enrollments
        , count(distinct micromasters_enrollments.user_email) as unique_users
        , count(distinct micromasters_enrollments.user_address_country) as unique_countries
        , count(distinct case
            when micromasters_enrollments.courserunenrollment_enrollment_mode = 'verified'
                then (micromasters_enrollments.courserun_readable_id || micromasters_enrollments.user_email)
        end) as verified_enrollments
        , count(distinct case
            when micromasters_enrollments.courserunenrollment_enrollment_mode = 'verified'
                then micromasters_enrollments.user_email
        end) as unique_verified_users
    from micromasters_enrollments
    inner join mitx_programs
        on
            micromasters_enrollments.micromasters_program_id = mitx_programs.micromasters_program_id
            or micromasters_enrollments.mitxonline_program_id = mitx_programs.mitxonline_program_id
    group by 1
)

, course_certs as (
    select
        case when
            mitx_programs.is_dedp_program = true then 'Data, Economics, and Design of Policy'
        else micromasters_course_certificates.program_title end as program_title
        , count(
            distinct
            micromasters_course_certificates.courserun_readable_id
            || micromasters_course_certificates.user_email
        ) as course_certificates
        , count(distinct micromasters_course_certificates.user_email) as unique_course_certificate_earners
    from micromasters_course_certificates
    inner join mitx_programs
        on
            micromasters_course_certificates.micromasters_program_id = mitx_programs.micromasters_program_id
            or micromasters_course_certificates.mitxonline_program_id = mitx_programs.mitxonline_program_id
    group by 1
)

, program_certs as (
    select
        mitx_programs.program_title
        , count(distinct micromasters_program_certificates.user_email) as program_certificates
    from micromasters_program_certificates
    inner join mitx_programs
        on
            micromasters_program_certificates.micromasters_program_id = mitx_programs.micromasters_program_id
            or micromasters_program_certificates.mitxonline_program_id = mitx_programs.mitxonline_program_id
    group by 1
)

select
    enrollments.program_title
    , enrollments.total_enrollments
    , enrollments.unique_users
    , enrollments.unique_countries
    , enrollments.verified_enrollments
    , enrollments.unique_verified_users
    , course_certs.course_certificates
    , course_certs.unique_course_certificate_earners
    , program_certs.program_certificates
from enrollments
left join course_certs
    on enrollments.program_title = course_certs.program_title
left join program_certs
    on enrollments.program_title = program_certs.program_title
